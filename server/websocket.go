package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

// Handler is the interface of any websocket consumer
// In practice, this is either sub or pub.
type Handler interface {
	OnWrite(ctx context.Context, w io.Writer) error
	OnRead(ctx context.Context, r io.Reader) error
}

type WebsocketOptions struct {
	PingInterval time.Duration
	PongTimeout  time.Duration
}

func (wc *WebsocketOptions) Validate() error {
	if wc.PingInterval < time.Second {
		return fmt.Errorf("ping_interval must be at least 1s")
	}

	if wc.PongTimeout < time.Second {
		return fmt.Errorf("pong_timeout must be at least 1s")
	}

	return nil
}

func DefaultWebsocketOptions() WebsocketOptions {
	return WebsocketOptions{
		PingInterval: 5 * time.Second,
		PongTimeout:  15 * time.Second,
	}
}

//////////////

type WebsocketConn struct {
	opts    *WebsocketOptions
	conn    *websocket.Conn
	cancel  func()
	abortCh chan error
}

func NewWebsocketConn(conn *websocket.Conn, cfg *WebsocketOptions) *WebsocketConn {
	return &WebsocketConn{
		opts:    cfg,
		conn:    conn,
		abortCh: make(chan error, 1),
	}
}

func (h *WebsocketConn) Options() *WebsocketOptions {
	return h.opts
}

// abort aborts the current connection by canceling the context
// and sending a close message (as best effort) to the client.
// It may only be called on the write side. Use abortCh for the rest.
func (h *WebsocketConn) abort(err error) {
	if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		return
	}

	if h.cancel == nil {
		return
	}

	// Try to write error message to client, but there's no guarantee the
	// connection is still working, so no error checking is done.
	if err == nil {
		_ = h.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseMessage,
				"connection terminated normally",
			),
		)
		slog.Warn("terminated connection")
	} else {
		_ = h.conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseAbnormalClosure,
				fmt.Sprintf("connection aborted: %v", err),
			),
		)
		slog.Warn("aborted connection", "err", err)
	}

	h.cancel()
	h.cancel = nil

	time.Sleep(50 * time.Millisecond)
	h.conn.Close()
}

func (h *WebsocketConn) serveWrites(ctx context.Context, handler Handler) {
	ticker := time.NewTicker(h.opts.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.abort(fmt.Errorf("context canceled: %w", ctx.Err()))
			return
		case err := <-h.abortCh:
			h.abort(err)
			return
		case <-ticker.C:
			if err := h.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.abort(fmt.Errorf("failed to ping client: %w", err))
				return
			}
		default:
			// NOTE: assumption is that writer() only blocks for a short amount of time.
			// (e.g. if no data is available to write, but not longer than the ping interval)
			if err := handler.OnWrite(ctx, &websocketWriter{conn: h.conn}); err != nil {
				h.abort(fmt.Errorf("failed to write to client: %w", err))
				return
			}
		}
	}
}

func (h *WebsocketConn) serveReads(ctx context.Context, handler Handler) {
	for {
		if err := h.conn.SetReadDeadline(time.Now().Add(h.opts.PongTimeout)); err != nil {
			h.abortCh <- fmt.Errorf("failed to set deadline: %w", err)
			return
		}

		_, r, err := h.conn.NextReader()
		if err != nil {
			h.abortCh <- fmt.Errorf("failed to read next message: %w", err)
			return
		}

		// NOTE:We assume that OnRead() does not immediately return and does not block too long.
		if err := handler.OnRead(ctx, r); err != nil {
			h.abortCh <- fmt.Errorf("failed to handle what we read: %w", err)
			return
		}
	}
}

func (h *WebsocketConn) Serve(ctx context.Context, handler Handler) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	defer h.abort(nil)

	// Whenever an alive connection returns a PONG, this function is called.
	// We use this to check if the peer is still alive and well. If not
	// we clean up the connection to save resources.
	h.conn.SetPongHandler(func(string) error {
		// extend the read deadline for some time:
		if err := h.conn.SetReadDeadline(time.Now().Add(h.opts.PongTimeout)); err != nil {
			h.abort(fmt.Errorf("failed to set deadline: %w", err))
			return err
		}

		return nil
	})

	go h.serveReads(ctx, handler)

	// This goroutine handles the write part of the websocket:
	// Apparently you are supposed to do the writing on the socket in the same
	// go-routine: https://github.com/gorilla/websocket/issues/595
	//
	// We also send pings regularly to learn if the client is still alive.
	// See the SetPongHandler() call below.
	h.serveWrites(ctx, handler)
}

// small wrapper around websocket.Conn to allow easy usage for OnWrite()
type websocketWriter struct {
	conn *websocket.Conn
}

func (ww *websocketWriter) Write(buf []byte) (int, error) {
	err := ww.conn.WriteMessage(websocket.BinaryMessage, buf)
	return len(buf), err
}

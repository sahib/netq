package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

type Handler interface {
	OnWrite(ctx context.Context, w io.Writer) error
	OnRead(ctx context.Context, data []byte) error
}

type WebsocketConfig struct {
	PingInterval time.Duration
	PongTimeout  time.Duration
}

func (wc *WebsocketConfig) Validate() error {
	if wc.PingInterval < time.Second {
		return fmt.Errorf("ping_interval must be at least 1s")
	}

	if wc.PongTimeout < time.Second {
		return fmt.Errorf("pong_timeout must be at least 1s")
	}

	return nil
}

func WebsocketConfigDefault() *WebsocketConfig {
	return &WebsocketConfig{
		PingInterval: 5 * time.Second,
		PongTimeout:  15 * time.Second,
	}
}

type WebsocketConn struct {
	cfg    *WebsocketConfig
	conn   *websocket.Conn
	cancel func()
}

func NewWebsocketConn(conn *websocket.Conn, cfg *WebsocketConfig) *WebsocketConn {
	return &WebsocketConn{
		cfg:  cfg,
		conn: conn,
	}
}

func (h *WebsocketConn) abort(err error) {
	if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		return
	}

	if h.cancel != nil {
		defer h.cancel()
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

}

func (h *WebsocketConn) serveWrites(ctx context.Context, cancel func(), handler Handler) {
	ticker := time.NewTicker(h.cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.abort(fmt.Errorf("context canceled: %w", ctx.Err()))
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

func (h *WebsocketConn) serveReads(ctx context.Context, cancel func(), handler Handler) {
	for {
		if err := h.conn.SetReadDeadline(time.Now().Add(h.cfg.PongTimeout)); err != nil {
			h.abort(fmt.Errorf("failed to set deadline: %w", err))
			return
		}

		_, r, err := h.conn.NextReader()
		if err != nil {
			h.abort(fmt.Errorf("failed to read next message: %w", err))
			return
		}

		// OPTIMIZATION: Re-use a buffer instead of allocating one all the
		// time. This can be done by having a large buffer and using it in a
		// ringbuffer like fashion. For now ReadAll is fine enough.
		data, err := io.ReadAll(r)
		if err != nil {
			h.abort(fmt.Errorf("failed to read: %v", err))
			return
		}

		if err := handler.OnRead(ctx, data); err != nil {
			h.abort(fmt.Errorf("failed to handle what we read: %w", err))
			return
		}
	}
}

func (h *WebsocketConn) Serve(ctx context.Context, handler Handler) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	defer h.abort(nil)

	// This goroutine handles the write part of the websocket:
	// Apparently you are supposed to do the writing on the socket in the same
	// go-routine: https://github.com/gorilla/websocket/issues/595
	//
	// We also send pings regularly to learn if the client is still alive.
	// See the SetPongHandler() call below.
	go h.serveWrites(ctx, cancel, handler)

	// Whenever an alive connection returns a PONG, this function is called.
	// We use this to check if the peer is still alive and well. If not
	// we clean up the connection to save resources.
	h.conn.SetPongHandler(func(string) error {
		// extend the read deadline for some time:
		if err := h.conn.SetReadDeadline(time.Now().Add(h.cfg.PongTimeout)); err != nil {
			h.abort(fmt.Errorf("failed to set deadline: %w", err))
		}

		return nil
	})

	h.serveReads(ctx, cancel, handler)
}

// small wrapper around websocket.Conn to allow easy usage for OnWrite()
type websocketWriter struct {
	conn *websocket.Conn
}

func (ww *websocketWriter) Write(buf []byte) (int, error) {
	err := ww.conn.WriteMessage(websocket.BinaryMessage, buf)
	return len(buf), err
}

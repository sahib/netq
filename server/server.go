package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sahib/netq/protocol"
)

var (
	// We set thee buffer pool to safe some memory, as described in this commit:
	// https://github.com/gorilla/websocket/commit/80393295c1185e50d0b784d4bc5ffaa918d187b9
	upgrader = &websocket.Upgrader{
		ReadBufferSize:  256,
		WriteBufferSize: 256,
		WriteBufferPool: &sync.Pool{},
	}
)

type Options struct {
	Addr             string
	StorageDir       string
	WebsocketOptions protocol.WebsocketOptions
	TopicOptions     TopicOptions
	SubOptions       SubOptions
}

func DefaultOptions() *Options {
	return &Options{
		Addr:             "ws://127.0.0.1:9876",
		StorageDir:       "/var/netq",
		WebsocketOptions: protocol.DefaultWebsocketOptions(),
		TopicOptions:     DefaultTopicOptions(),
		SubOptions:       DefaultSubOptions(),
	}
}

func (c *Options) Validate() error {
	if _, err := url.Parse(c.Addr); err != nil {
		return fmt.Errorf("addr (%v) is invalid: %w", c.Addr, err)
	}

	if err := os.MkdirAll(c.StorageDir, 0700); err != nil {
		return fmt.Errorf("failed to create storage dir (%v): %w", c.StorageDir, err)
	}

	if err := c.TopicOptions.Validate(); err != nil {
		return fmt.Errorf("topic options: %w", err)
	}

	if err := c.WebsocketOptions.Validate(); err != nil {
		return fmt.Errorf("websocket options: %w", err)
	}

	if err := c.SubOptions.Validate(); err != nil {
		return fmt.Errorf("sub options: %w", err)
	}

	return nil
}

type Server struct {
	ctx    context.Context
	cancel func()
	srv    *http.Server
	cfg    *Options
	topics *Topics
}

func NewServer(ctx context.Context, cfg *Options) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	u, err := url.Parse(cfg.Addr)
	if err != nil {
		return nil, err
	}

	slog.Info("configuration seems to be valid")
	ctx, cancel := context.WithCancel(ctx)
	srv := &http.Server{
		Addr:         u.Host,
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	topics := NewTopics(cfg.StorageDir, cfg.TopicOptions)
	return &Server{
		ctx:    ctx,
		cancel: cancel,
		cfg:    cfg,
		srv:    srv,
		topics: topics,
	}, nil
}

func (s *Server) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/sub", s.updateRequestToWebsocket)
	mux.HandleFunc("/pub", s.updateRequestToWebsocket)
	mux.HandleFunc("/cmd", s.updateRequestToWebsocket)

	// TODO: Implement prometheus exporter
	// Metrics (should be the same as a stats endpoint):
	// - Number of topics
	// - Number of connections
	// - Per-topic:
	//    - Name
	//    - Size in Bytes
	//    - Number of messages waiting
	//    - Number of messages unacked
	//    - Number of messages pushed
	//    - Number of messages popped

	s.srv.Handler = mux

	// run server in background:
	var srvErrCh chan error
	go func() {
		defer slog.Debug("server exited")
		srvErrCh <- s.srv.ListenAndServe()
	}()

	slog.Info("server is ready and running", "addr", s.cfg.Addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	var err error
	select {
	case <-s.ctx.Done():
		slog.Info("shutting down due to close")
	case <-sigCh:
		// Ctrl-C received.
		slog.Info("SIGINT received, shutting down")
	case err = <-srvErrCh:
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			// stupid hack to get rid of this rather pointless message:
			err = nil
		} else if err != nil {
			slog.Warn("failed to serve", "err", err)
		}
	}

	// allow up to 5s for a proper shutdown:
	shutdownCtx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	return errors.Join(
		err,
		s.srv.Shutdown(shutdownCtx),
	)
}

func (s *Server) Close() error {
	s.cancel()
	return nil
}

func (s *Server) topicForRequest(vals url.Values) (*Topic, TopicSpec, error) {
	topicSpecRaw := vals.Get("topic")
	topicSpec := TopicSpec(topicSpecRaw)
	if err := topicSpec.Validate(); err != nil {
		return nil, "", err
	}

	topic, err := s.topics.Ref(topicSpec)
	return topic, topicSpec, err
}

func (s *Server) updateRequestToWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Fprintf(w, "failed to upgrade: %v", err)
		return
	}

	wsh := protocol.NewWebsocketConn(conn, &s.cfg.WebsocketOptions)

	vals := r.URL.Query()
	topic, topicSpec, err := s.topicForRequest(vals)
	if err != nil {
		wsh.Abort(nil, err)
		s.topics.Unref(topicSpec)
		return
	}

	var handler protocol.Handler
	switch r.URL.Path {
	case "/sub":
		// Allow overwriting parts of the configuration via query params:
		subOpts, err := s.cfg.SubOptions.OverlayWithURLParams(vals)
		if err != nil {
			wsh.Abort(nil, err)
			s.topics.Unref(topicSpec)
			return
		}

		handler, err = NewSubHandler(topic, topicSpec, subOpts)
		if err != nil {
			wsh.Abort(nil, err)
			s.topics.Unref(topicSpec)
			return
		}
	case "/pub":
		handler = NewPubHandler(topic)
	default:
		wsh.Abort(nil, fmt.Errorf("invalid handler path: %s", r.URL.Path))
		s.topics.Unref(topicSpec)
		return
	}

	// NOTE: Let the handler return, we'll handle the websocket in another go routine.
	// The http webserver can then GC open buffers.
	go func() {
		wsh.Serve(s.ctx, handler)
		s.topics.Unref(topicSpec)
	}()
}

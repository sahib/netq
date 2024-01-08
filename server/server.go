package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type Config struct {
	Addr         string
	Websocket    *WebsocketConfig
	StorageDir   string
	TopicOptions TopicOptions // TODO: call it consistently options or config, also make pointers consistent.
}

// TODO: validate method  for config.

func DefaultConfig() *Config {
	return &Config{
		Addr:       "127.0.0.1:9876",
		Websocket:  WebsocketConfigDefault(),
		StorageDir: "/var/netq", // TODO: good default?
	}
}

type Server struct {
	ctx    context.Context
	cancel func()
	srv    *http.Server
	cfg    *Config
	topics *Topics
}

func NewServer(ctx context.Context, cfg *Config) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)
	srv := &http.Server{
		Addr:         cfg.Addr,
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

	// TODO: Handlers for statistics (native prometheus support?)
	s.srv.Handler = mux

	// run server in background:
	var srvErrCh chan error
	go func() {
		srvErrCh <- s.srv.ListenAndServe()
	}()

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

func (s *Server) topicForRequest(r *http.Request) (*Topic, TopicSpec, error) {
	vals := r.URL.Query()
	topicSpecRaw := vals.Get("topic")

	topicSpec := TopicSpec(topicSpecRaw)
	if err := topicSpec.Validate(); err != nil {
		return nil, "", err
	}

	topic, err := s.topics.Ref(topicSpec)
	return topic, topicSpec, err
}

func (s *Server) updateRequestToWebsocket(w http.ResponseWriter, r *http.Request) {
	upgrader := &websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Fprintf(w, "failed to upgrade: %v", err)
		return
	}

	wsh := NewWebsocketConn(conn, s.cfg.Websocket)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	topic, topicSpec, err := s.topicForRequest(r)
	if err != nil {
		wsh.abort(err)
		return
	}

	defer s.topics.Unref(topicSpec)

	var handler Handler
	switch r.URL.Path {
	case "sub":
		opts := SubOptions{
			BlockSize:  2000,
			MaxWait:    s.cfg.Websocket.PongTimeout / 2,
			AckTimeout: 15 * time.Second,
		}
		handler, err = NewSubHandler(r.Context(), topic, topicSpec, opts)
		if err != nil {
			wsh.abort(err)
			return
		}
	case "pub":
		handler = NewPubHandler(topic)
	default:
		wsh.abort(fmt.Errorf("invalid handler path: %s", r.URL.Path))
	}

	wsh.Serve(ctx, handler)
}

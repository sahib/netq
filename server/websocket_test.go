package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func nextFreePort(t *testing.T) int {
	lst, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lst.Close()
	return lst.Addr().(*net.TCPAddr).Port
}

type TestCtx struct {
	wg     *sync.WaitGroup
	srv *Server
	wsConn *websocket.Conn
}

func (tc *TestCtx) Teardown(t *testing.T) {
	require.NoError(t, tc.wsConn.Close())
	require.NoError(t, tc.srv.Close())
	tc.wg.Wait()
}

func Setup(t *testing.T) *TestCtx {
	port := nextFreePort(t)
	cfg := DefaultOptions()
	cfg.Addr = fmt.Sprintf("localhost:%d", port)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // max time for test.
	defer cancel()

	srv, err := NewServer(ctx, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer srv.Close()
		require.NoError(t, srv.Serve())
	}()

	url := fmt.Sprintf("ws://localhost:%d/sub", port)
	wsConn, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)

	return &TestCtx{
		wg:     &wg,
		srv: srv,
		wsConn: wsConn,
	}
}

func TestWebsocketHandler(t *testing.T) {
	tc := Setup(t)
	defer tc.Teardown(t)

	for idx := 0; idx < 10; idx++ {
		require.NoError(t, tc.wsConn.WriteMessage(
			websocket.BinaryMessage,
			[]byte("hello world"),
		))
		time.Sleep(time.Second)
	}

}

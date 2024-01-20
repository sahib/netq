package server

import (
	"context"
	"fmt"
	"net"
	"os"
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
	srv    *Server
	wsConn *websocket.Conn
	tmpDir string
	cancel func()
}

func (tc *TestCtx) Teardown(t *testing.T) {
	tc.cancel()
	tc.wg.Wait()
	require.NoError(t, os.RemoveAll(tc.tmpDir))
	require.NoError(t, tc.wsConn.Close())
	require.NoError(t, tc.srv.Close())

}

func Setup(t *testing.T) *TestCtx {
	port := nextFreePort(t)

	tmpDir, err := os.MkdirTemp("", "netq-test-ws")
	require.NoError(t, err)

	cfg := DefaultOptions()
	cfg.Addr = fmt.Sprintf("localhost:%d", port)
	cfg.StorageDir = tmpDir

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // max time for test.
	srv, err := NewServer(ctx, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer srv.Close()
		require.NoError(t, srv.Serve())
	}()

	url := fmt.Sprintf("ws://localhost:%d/sub?topic=foo", port)

	var wsConn *websocket.Conn
	for idx := 0; idx < 10; idx++ {
		wsConn, _, err = websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		break
	}

	require.NotNil(t, wsConn)

	return &TestCtx{
		wg:     &wg,
		srv:    srv,
		wsConn: wsConn,
		tmpDir: tmpDir,
		cancel: cancel,
	}
}

func TestWebsocketHandler(t *testing.T) {
	tc := Setup(t)
	defer tc.Teardown(t)

	tc.wsConn.SetCloseHandler(func(code int, text string) error {
		fmt.Println(code, text)
		return nil
	})

	require.NoError(t, tc.wsConn.WriteMessage(
		websocket.BinaryMessage,
		[]byte("hello world"),
	))

	// Since we wrote nothing that netq could parse, we should
	// get an error.
	_, _, err := tc.wsConn.ReadMessage()
	require.Error(t, err)
	require.ErrorContains(t, err, "1006")
}

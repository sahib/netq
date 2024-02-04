package protocol

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type DummyServer struct {
	t    *testing.T
	opts WebsocketOptions
	addr string
	ctx  context.Context
}

type EchoHandler struct {
	writeCh chan []byte
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{
		writeCh: make(chan []byte, 1),
	}
}

func (eh *EchoHandler) OnWrite(ctx context.Context, w io.Writer) error {
	select {
	case data := <-eh.writeCh:
		_, err := w.Write(data)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (eh *EchoHandler) OnRead(ctx context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	eh.writeCh <- data
	return nil
}

func (eh *EchoHandler) OnClose(err error) {}

func NewDummyServer(ctx context.Context, t *testing.T, addr string, opts WebsocketOptions) *DummyServer {
	return &DummyServer{
		t:    t,
		ctx:  ctx,
		addr: addr,
		opts: opts,
	}
}

func (ds *DummyServer) Serve() error {
	return http.ListenAndServe(ds.addr, http.HandlerFunc(ds.updateRequestToWebsocket))
}

func (ds *DummyServer) updateRequestToWebsocket(w http.ResponseWriter, r *http.Request) {
	upgrader := &websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Fprintf(w, "failed to upgrade: %v", err)
		return
	}

	wsh := NewWebsocketConn(conn, &ds.opts)
	wsh.Serve(ds.ctx, NewEchoHandler())
}

// TODO: Make this a testutil.
func nextFreePort(t *testing.T) int {
	lst, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer lst.Close()
	return lst.Addr().(*net.TCPAddr).Port
}

type TestCtx struct {
	wg     *sync.WaitGroup
	srv    *DummyServer
	wsConn *websocket.Conn
	tmpDir string
	cancel func()
}

func (tc *TestCtx) Teardown(t *testing.T) {
	tc.cancel()
	tc.wg.Wait()
	require.NoError(t, os.RemoveAll(tc.tmpDir))
	require.NoError(t, tc.wsConn.Close())
}

func Setup(t *testing.T) *TestCtx {
	port := nextFreePort(t)

	tmpDir, err := os.MkdirTemp("", "netq-test-ws")
	require.NoError(t, err)

	cfg := DefaultWebsocketOptions()
	addr := fmt.Sprintf("localhost:%d", port)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // max time for test.
	srv := NewDummyServer(ctx, t, addr, cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
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

package client

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sahib/netq/protocol"
)

type cmdPackage struct {
	ID      uint64
	Cmd     protocol.CmdType
	Payload []byte
}

type Cmd struct {
	cancel func()
	conn   *websocket.Conn
	cmdEnc protocol.CmdEncoder
	currID uint64

	// Make the async protocol look like sync:
	packCh   chan cmdPackage
	respCond *sync.Cond
	resp     map[uint64]cmdPackage
}

type cmdImpl struct {
	*Cmd
}

func (c *cmdImpl) OnWrite(ctx context.Context, w io.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pack := <-c.packCh:
		return c.conn.WriteMessage(
			websocket.BinaryMessage,
			c.cmdEnc.Encode(
				pack.ID,
				pack.Cmd,
				pack.Payload,
			),
		)
	}
}

func (c *cmdImpl) OnRead(ctx context.Context, r io.Reader) error {
	message, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	id, cmd, payload, err := protocol.DecodeCmd(message)
	if err != nil {
		return err
	}

	c.respCond.L.Lock()
	c.resp[id] = cmdPackage{
		ID:      id,
		Cmd:     cmd,
		Payload: payload,
	}
	c.respCond.Broadcast()
	c.respCond.L.Unlock()

	return nil
}

func (c *cmdImpl) OnClose(err error) {
}

/////////////

func (c *Cmd) send(ctx context.Context, pack cmdPackage) (cmdPackage, error) {
	c.packCh <- pack

	var ok bool
	c.respCond.L.Lock()
	for {
		pack, ok = c.resp[pack.ID]
		if ok {
			break
		}

		select {
		case <-ctx.Done():
			return cmdPackage{}, ctx.Err()
		default:
			// wait until response available.
			c.respCond.Wait()
		}
	}
	c.respCond.L.Unlock()
	return pack, nil
}

func (c *Cmd) nextID() uint64 {
	c.currID++
	return c.currID
}

func (c *Cmd) Ping(ctx context.Context) error {
	_, err := c.send(ctx, cmdPackage{
		ID:      c.nextID(),
		Cmd:     protocol.CmdTypePing,
		Payload: []byte{}, // no payload needed.
	})
	return err
}

func (c *Cmd) TopicClear(ctx context.Context, topic string) (int, error) {
	payload, err := json.Marshal(protocol.CmdRequestTopicClear{
		Topic: topic,
	})

	if err != nil {
		return 0, err
	}

	pack, err := c.send(ctx, cmdPackage{
		ID:      c.nextID(),
		Cmd:     protocol.CmdTypeTopicClear,
		Payload: payload,
	})

	if err != nil {
		return 0, err
	}

	var resp protocol.CmdResponseTopicClear
	if err := json.Unmarshal(pack.Payload, &resp); err != nil {
		return 0, err
	}

	return resp.NCleared, nil
}

func (c *Client) Cmd(ctx context.Context) (*Cmd, error) {
	ctx, cancel := context.WithCancel(ctx)
	url := c.buildURL("/cmd", nil)
	conn, err := c.connect(ctx, url)
	if err != nil {
		cancel()
		return nil, err
	}

	wh := protocol.NewWebsocketConn(conn, &c.opts.WebsocketOptions)

	cmd := &Cmd{
		cancel:   cancel,
		conn:     conn,
		cmdEnc:   protocol.CmdEncoder{},
		resp:     make(map[uint64]cmdPackage),
		respCond: sync.NewCond(&sync.Mutex{}),
		packCh:   make(chan cmdPackage, 1),
	}

	go wh.Serve(ctx, &cmdImpl{Cmd: cmd})
	return cmd, nil
}

func (s *Cmd) Close() error {
	s.cancel()
	return s.conn.Close()
}

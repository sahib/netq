package client

import (
	"context"
	"encoding/json"

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
	client *Client
	url    string
	cmdEnc protocol.CmdEncoder
	currID uint64
}

func (c *Cmd) send(ctx context.Context, pack cmdPackage) (cmdPackage, error) {
	data := c.cmdEnc.Encode(
		pack.ID,
		pack.Cmd,
		pack.Payload,
	)

	if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		c.conn = c.client.reconnect(ctx, err, c.url)
		if c.conn == nil {
			return cmdPackage{}, err
		}
	}

	_, message, err := c.conn.ReadMessage()
	if err != nil {
		// attempt reconnect:
		c.conn = c.client.reconnect(ctx, err, c.url)
		if c.conn == nil {
			// return original error in this case:
			return cmdPackage{}, err
		}
	}

	id, cmd, payload, err := protocol.DecodeCmd(message)
	if err != nil {
		return cmdPackage{}, err
	}

	return cmdPackage{
		ID:      id,
		Cmd:     cmd,
		Payload: payload,
	}, nil
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

	return &Cmd{
		client: c,
		cancel: cancel,
		conn:   conn,
		url:    url,
		cmdEnc: protocol.CmdEncoder{},
	}, nil
}

func (s *Cmd) Close() error {
	s.cancel()
	return s.conn.Close()
}

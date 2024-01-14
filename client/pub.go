package client

import (
	"context"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/sahib/netq/protocol"
)

type Pub struct {
	pubid   uint64
	client  *Client
	cancel  func()
	batchCh chan *Batch
	url     string
}

func (p *Pub) ioLoop(ctx context.Context, conn *websocket.Conn, onAck func(id uint64) error) {
	defer conn.Close()
	var batchEnc protocol.BatchEncoder

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-p.batchCh:
			data := batchEnc.Encode(batch.ID, batch.Items)
			if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				conn = p.client.reconnect(ctx, err, p.url)
				if conn == nil {
					return
				}
			}
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				conn = p.client.reconnect(ctx, err, p.url)
				if conn == nil {
					return
				}
				return
			}

			if onAck == nil {
				continue
			}

			id, err := protocol.DecodeAck(message)
			if err != nil {
				p.client.onError(err, false)
				return
			}

			if err := onAck(id); err != nil {
				p.client.onError(err, false)
				return
			}
		}
	}
}

func (c *Client) Pub(ctx context.Context, topic string, onAck func(id uint64) error) (*Pub, error) {
	ctx, cancel := context.WithCancel(ctx)
	url := c.buildURL("/pub", map[string]string{"topic": topic})
	conn, err := c.connect(ctx, url)
	if err != nil {
		cancel()
		return nil, err
	}

	pub := &Pub{
		client:  c,
		cancel:  cancel,
		batchCh: make(chan *Batch, 10),
		url:     url,
	}

	go pub.ioLoop(ctx, conn, onAck)
	return pub, nil
}

func (p *Pub) Push(items Items) uint64 {
	id := atomic.AddUint64(&p.pubid, 1)
	p.batchCh <- &Batch{
		ID:    id,
		Items: items,
	}
	return id
}

func (p *Pub) Close() error {
	p.cancel()
	return nil
}

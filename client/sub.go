package client

import (
	"context"

	"github.com/gorilla/websocket"
	"github.com/sahib/netq/protocol"
)

type Sub struct {
	client *Client
	cancel func()
	ackCh  chan uint64
	url    string
}

type Batch struct {
	Sub   *Sub
	ID    uint64
	Items Items
}

// Ack does the same as Sub.Ack() but can be conveniently called on a batch.
func (b *Batch) Ack() {
	b.Sub.Ack(b.ID)
}

func (s *Sub) ioLoop(ctx context.Context, conn *websocket.Conn, onMessage func(b *Batch) error) {
	defer conn.Close()
	var ackEnc protocol.AckEncoder

	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.ackCh:
			if err := conn.WriteMessage(websocket.BinaryMessage, ackEnc.Encode(id)); err != nil {
				conn = s.client.reconnect(ctx, err, s.url)
				if conn == nil {
					return
				}
			}
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				conn = s.client.reconnect(ctx, err, s.url)
				if conn == nil {
					return
				}
			}

			if onMessage == nil {
				continue
			}

			id, items, err := protocol.DecodeBatch(message)
			if err != nil {
				// reconnecting probably won't help if we can't decode a message.
				s.client.onError(err, false)
				return
			}

			// TODO: We do assume here that the message handler does not block
			// for extended times, as this will cause the connection to timeout
			// after a while. Have an option to do it in the background + another go?
			// Or maybe just pass a context for each batch?
			// Or maybe just document it and let it be?
			batch := Batch{
				Sub:   s,
				ID:    id,
				Items: items,
			}

			batch.

			if err := onMessage(&batch); err != nil {
				// user defined errors also do not indicate that we should reconnect.
				s.client.onError(err, false)
				return
			}
		}
	}
}

func (c *Client) Sub(ctx context.Context, topic string, onMessage func(b *Batch) error) (*Sub, error) {
	ctx, cancel := context.WithCancel(ctx)
	url := c.buildURL("/sub", map[string]string{"topic": topic})
	conn, err := c.connect(ctx, url)
	if err != nil {
		cancel()
		return nil, err
	}

	sub := &Sub{
		client: c,
		cancel: cancel,
		url:    url,
		ackCh:  make(chan uint64, 10),
	}

	go sub.ioLoop(ctx, conn, onMessage)
	return sub, nil
}

func (s *Sub) ResendUnacked() {
	s.ackCh <- 0
}

func (s *Sub) Ack(id uint64) {
	if id == 0 {
		return
	}

	s.ackCh <- id
}

func (s *Sub) Close() error {
	s.cancel()
	return nil
}

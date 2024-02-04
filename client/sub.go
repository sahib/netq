package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/sahib/netq/protocol"
)

type Batch struct {
	Sub   *Sub
	ID    uint64
	Items Items
}

// Ack does the same as Sub.Ack() but can be conveniently called on a batch.
func (b *Batch) Ack() {
	b.Sub.Ack(b.ID)
}

type Sub struct {
	cancel    func()
	ackCh     chan uint64
	onMessage func(b *Batch) error
	ackEnc    protocol.AckEncoder
}

type subImpl struct {
	*Sub
}

// TODO: Those methods are public. :/
func (s *subImpl) OnRead(ctx context.Context, r io.Reader) error {
	message, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if s.onMessage == nil {
		return nil
	}

	id, items, err := protocol.DecodeBatch(message)
	if err != nil {
		return err
	}

	// TODO: We do assume here that the message handler does not block
	// for extended times, as this will cause the connection to timeout
	// after a while. Have an option to do it in the background + another go?
	// Or maybe just pass a context for each batch?
	// Or maybe just document it and let it be?
	batch := Batch{
		Sub:   s.Sub,
		ID:    id,
		Items: items,
	}

	fmt.Println("GOT BATCH", id)
	return s.onMessage(&batch)
}

func (s *subImpl) OnWrite(ctx context.Context, w io.Writer) error {
	// TODO: Move ping to general handler.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case id := <-s.ackCh:
		fmt.Println("ACK", id)
		_, err := w.Write(s.ackEnc.Encode(id))
		return err
	case <-time.After(5 * time.Second):
		// TODO: is this branch necessary?
		return nil
	}
}

func (s *subImpl) OnClose(err error) {
}

func (c *Client) Sub(ctx context.Context, topic string, onMessage func(b *Batch) error) (*Sub, error) {
	ctx, cancel := context.WithCancel(ctx)
	url := c.buildURL("/sub", map[string]string{"topic": topic})
	conn, err := c.connect(ctx, url)
	if err != nil {
		cancel()
		return nil, err
	}

	wh := protocol.NewWebsocketConn(conn, &c.opts.WebsocketOptions)

	sub := &Sub{
		cancel:    cancel,
		ackCh:     make(chan uint64, 10),
		onMessage: onMessage,
	}

	go wh.Serve(ctx, &subImpl{Sub: sub})
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

package client

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/sahib/netq/protocol"
)

type Pub struct {
	pubid    uint64
	cancel   func()
	batchCh  chan *Batch
	batchEnc protocol.BatchEncoder
	onAck    func(id uint64) error
}

// make sure that On{Write,Read,Close} are not public methods.
type pubImpl struct {
	*Pub
}

func (p *pubImpl) OnWrite(ctx context.Context, w io.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case batch := <-p.batchCh:
		data := p.batchEnc.Encode(batch.ID, batch.Items)
		_, err := w.Write(data)
		return err
	case <-time.After(5 * time.Second):
		// TODO: is this branch necessary?
		return nil
	}
}

func (p *pubImpl) OnRead(ctx context.Context, r io.Reader) error {
	message, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if p.onAck == nil {
		return nil
	}

	id, err := protocol.DecodeAck(message)
	if err != nil {
		return err
	}

	return p.onAck(id)
}

func (p *pubImpl) OnClose(err error) {
	// TODO: Check if canceled. If not, reconnect?
}

func (c *Client) Pub(ctx context.Context, topic string, onAck func(id uint64) error) (*Pub, error) {
	ctx, cancel := context.WithCancel(ctx)
	url := c.buildURL("/pub", map[string]string{"topic": topic})
	conn, err := c.connect(ctx, url)
	if err != nil {
		cancel()
		return nil, err
	}

	wh := protocol.NewWebsocketConn(conn, &c.opts.WebsocketOptions)

	pub := &Pub{
		cancel:  cancel,
		batchCh: make(chan *Batch, 10),
		onAck:   onAck,
	}

	go wh.Serve(ctx, &pubImpl{Pub: pub})
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

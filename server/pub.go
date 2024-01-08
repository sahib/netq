package server

import (
	"context"
	"io"
	"math"
	"time"

	"github.com/sahib/netq/protocol"
	"github.com/sahib/timeq"
)

const pubAckTimeout = 1 * time.Second

type PubHandler struct {
	topic      *Topic
	lastPubAck time.Time
	lastPubID  uint32
	currPubID  uint32
	enc        protocol.AckEncoder
}

func NewPubHandler(topic *Topic) *PubHandler {
	return &PubHandler{
		topic:      topic,
		lastPubAck: time.Now(),
		lastPubID:  math.MaxUint32,
		currPubID:  0,
	}
}

func (ph *PubHandler) OnRead(ctx context.Context, data []byte) error {
	id, items, err := protocol.DecodeBatch(data)
	if err != nil {
		return err
	}

	ph.currPubID = id
	return ph.topic.Push(items)
}

func (ph *PubHandler) OnWrite(ctx context.Context, w io.Writer) error {
	diff := time.Since(ph.lastPubAck)
	if diff > 0 {
		// make sure to not spam pub acks all the time:
		time.Sleep(diff)
	}

	ph.lastPubAck = time.Now()
	if ph.lastPubID == ph.currPubID {
		// nothing changed.
		return nil
	}

	_, err := w.Write(ph.enc.Encode(timeq.Key(ph.currPubID)))
	ph.lastPubID = ph.currPubID
	return err
}

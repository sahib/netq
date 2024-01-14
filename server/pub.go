package server

import (
	"bytes"
	"context"
	"io"
	"math"
	"time"

	"github.com/sahib/netq/protocol"
)

type PubHandler struct {
	topic      *Topic
	lastPubAck time.Time
	lastPubID  uint64
	currPubID  uint64
	enc        protocol.AckEncoder
	wsBuf      bytes.Buffer
	copyBuf    []byte
}

func NewPubHandler(topic *Topic) *PubHandler {
	return &PubHandler{
		topic:      topic,
		lastPubAck: time.Now(),
		lastPubID:  math.MaxUint32,
		currPubID:  0,
		copyBuf:    make([]byte, 16*1024),
	}
}

func (ph *PubHandler) OnRead(ctx context.Context, r io.Reader) error {
	ph.wsBuf.Reset()
	if _, err := io.CopyBuffer(&ph.wsBuf, r, ph.copyBuf); err != nil {
		return err
	}

	id, items, err := protocol.DecodeBatch(ph.wsBuf.Bytes())
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

	_, err := w.Write(ph.enc.Encode(ph.currPubID))
	ph.lastPubID = ph.currPubID
	return err
}

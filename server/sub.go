package server

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/sahib/netq/protocol"
	"github.com/sahib/timeq"
)

type SubOptions struct {
	BlockSize  int
	MaxWait    time.Duration
	AckTimeout time.Duration
}

type SubHandler struct {
	ctx      context.Context
	fork     *TopicFork
	ackReset chan bool
	enc      protocol.BatchEncoder
	itemBuf  timeq.Items
	opts     SubOptions
	idCount  uint32
}

func NewSubHandler(ctx context.Context, topic *Topic, topicSpec TopicSpec, opts SubOptions) (*SubHandler, error) {
	// If topic spec included a fork name, we should use that and no consume
	// stuff from the main queue. If  the fork name was empty or default, then
	// we this return the consumer for the main queue anyways.
	fork, err := topic.Fork(topicSpec.ForkName())
	if err != nil {
		return nil, err
	}

	// Move items that were not yet acknowledged yet.
	unacked, err := fork.Restart()
	if err != nil {
		return nil, err
	}

	slog.Info("start of sub", "topic", topicSpec, "unacked", unacked)
	sh := &SubHandler{
		ctx:      ctx,
		opts:     opts,
		fork:     fork,
		ackReset: make(chan bool, 5),
		itemBuf:  make(timeq.Items, 0, opts.BlockSize),
	}

	go sh.handleAcks()
	return sh, nil
}

func (sh *SubHandler) handleAcks() {
	// Whenever we do not receive an acknowledgment for a certain time
	// we re-send the things in the unacked queue. Note that this is not a
	// per-message "AckTimeout" but a timeout per topic. This implies that,
	// if you keep sending acks and keep busy you will
	for {
		ackTimer := time.After(sh.opts.AckTimeout)
		select {
		case <-ackTimer:
			unacked, err := sh.fork.Restart()
			if err != nil {
				continue
			}

			slog.Info("did not receive ack in time - resending unacked", slog.Int("count", unacked))
		case <-sh.ctx.Done():
			return
		case <-sh.ackReset:
			// reset ack timeout.
		}
	}
}

func (sh *SubHandler) OnRead(ctx context.Context, data []byte) error {
	key, err := protocol.DecodeAck(data)
	if err != nil {
		return err
	}

	acked, err := sh.fork.Ack(key)
	if err != nil {
		return err
	}

	slog.Info("received acked", slog.Int("count", acked))
	sh.ackReset <- true
	return nil
}

func (sh *SubHandler) OnWrite(ctx context.Context, w io.Writer) error {
	return sh.fork.Pop(
		sh.opts.BlockSize,
		sh.itemBuf[:0],
		sh.opts.MaxWait,
		func(items timeq.Items) error {
			sh.idCount++
			_, err := w.Write(sh.enc.Encode(sh.idCount, items))
			return err
		},
	)
}

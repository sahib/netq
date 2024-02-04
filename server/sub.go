package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/sahib/netq/protocol"
	"github.com/sahib/timeq"
)

// TODO: Show topic name in logs.

type SubOptions struct {
	// AckTimeout defines the amount of time to wait before re-sending a batch
	// of messages.
	AckTimeout time.Duration

	// MaxUnacustore defines a maximum of messages that can be stored
	// before we move the unacked messages to the waiting queue again.
	// Set this to 0 to disable.
	MaxUnacked uint64

	// If we have enough data that we can send,
	// how should each block be at most?
	BlockSize uint
}

func (so *SubOptions) Validate() error {
	if so.BlockSize < 1 {
		return errors.New("block must be at least 1")
	}

	if so.AckTimeout < time.Second {
		return errors.New("ack timeout should be at least 1s")
	}

	return nil
}

func (so *SubOptions) OverlayWithURLParams(vals url.Values) (SubOptions, error) {
	copy := *so
	ackTimeoutStr := vals.Get("ack_timeout")
	if ackTimeoutStr != "" {
		ackTimeout, err := time.ParseDuration(ackTimeoutStr)
		if err != nil {
			return copy, fmt.Errorf("failed to parse ack_timeout=%v: %w", ackTimeoutStr, err)
		}

		copy.AckTimeout = ackTimeout
	}

	maxUnackedStr := vals.Get("max_unacked")
	if maxUnackedStr != "" {
		maxUnacked, err := strconv.ParseUint(maxUnackedStr, 10, 64)
		if err != nil {
			return copy, fmt.Errorf("failed to parse max_unacked=%v: %w", maxUnackedStr, err)
		}

		copy.MaxUnacked = maxUnacked
	}

	// make sure the values we set are actually in sane.
	return copy, copy.Validate()
}

func DefaultSubOptions() SubOptions {
	return SubOptions{
		BlockSize:  2000,
		MaxUnacked: 0, // this is a very use-case specific option.
		AckTimeout: 15 * time.Second,
	}
}

type SubHandler struct {
	fork     *TopicFork
	ackReset chan bool
	enc      protocol.BatchEncoder
	itemBuf  timeq.Items
	opts     SubOptions
	idCount  uint32
	wsBuf    bytes.Buffer
	copyBuf  []byte
}

func NewSubHandler(topic *Topic, topicSpec TopicSpec, opts SubOptions) (*SubHandler, error) {
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

	slog.Info("new subscription opened", "topic", topicSpec, "unacked", unacked)
	sh := &SubHandler{
		opts:     opts,
		fork:     fork,
		ackReset: make(chan bool, 5),
		itemBuf:  make(timeq.Items, 0, opts.BlockSize),
		copyBuf:  make([]byte, 16*1024),
	}

	go sh.handleAcks()
	return sh, nil
}

func (sh *SubHandler) handleAcks() {
	// Whenever we do not receive an acknowledgment for a certain time we
	// re-send the things in the unacked queue.
	//
	// Note that this is not a per-message "AckTimeout" but a timeout per
	// connection. This implies that, if you keep sending acks for unrelated
	// batches you will not get the unacked  batches only on the next
	// connection or, more commonly, that all other messages have to be
	// processed before the client gets send unacked messages.
	//
	// This is on purpose as setting a good AckTimeout in systems like NATS
	// is very hard, as you can easily get in a situation where the client gets
	// send old messages all the time because it is (for whatever) reason to slow
	// for some time. We try to favor processing as far as we can instead of hanging
	// the complete pipeline on a couple of messages.
	//
	// The client can additionally control when he wants a resend by sending an ACK
	// with a zero ID. This will trigger a resend immediately. Additionally, a total
	// amount of unacked messages can be configured which stop sending normal messages
	// and also trigger a resend.
	maxUnackedTckr := time.NewTicker(100 * time.Millisecond)
	ackTimer := time.After(sh.opts.AckTimeout)
	for {
		select {
		case <-ackTimer:
			// We did not receive an ack in a duration of AckTimeout
			// Continue with the restart after the select.
			ackTimer = time.After(sh.opts.AckTimeout)
		case v := <-sh.ackReset:
			if !v {
				// channel was closed.
				return
			}

			// reset the ack timeout because we received another ack.
			ackTimer = time.After(sh.opts.AckTimeout)
			continue
		// case <-sh.ctx.Done():
		// 	return
		case <-maxUnackedTckr.C:
			maxUnacked := sh.opts.MaxUnacked
			if maxUnacked == 0 {
				continue
			}

			if sh.fork.Unacked() < maxUnacked {
				// continue with the for loop.
				continue
			}

			slog.Info("too many unacked messages")
		}

		// put unacked messages into the waiting queue:
		unacked, err := sh.fork.Restart()
		if err != nil {
			slog.Warn("failed to restart", "err", err)
			continue
		}

		if unacked > 0 {
			slog.Info(
				"did not receive ack in time or too many unacked messages - resending unacked",
				slog.Int("count", unacked),
			)
		}
	}
}

func (sh *SubHandler) OnRead(ctx context.Context, r io.Reader) error {
	sh.wsBuf.Reset()
	if _, err := io.CopyBuffer(&sh.wsBuf, r, sh.copyBuf); err != nil {
		return err
	}

	id, err := protocol.DecodeAck(sh.wsBuf.Bytes())
	if err != nil {
		return err
	}

	sh.ackReset <- true
	if id == 0 {
		// A ACK with a id of zero indicates that the client wants to
		// have all unacked messages right now.
		var unacked int
		unacked, err = sh.fork.Restart()
		if err != nil {
			return err
		}

		slog.Info("received unack-request", slog.Int("count", unacked))
		return nil
	}

	fmt.Println("ACK", id)
	acked, err := sh.fork.Ack(id)
	if err != nil {
		return err
	}

	slog.Info("processed ack", slog.Int("count", acked))
	return nil
}

func (sh *SubHandler) OnWrite(ctx context.Context, w io.Writer) error {
	return sh.fork.Pop(
		ctx,
		int(sh.opts.BlockSize),
		sh.itemBuf[:0],
		time.Second,
		func(batchID uint64, items timeq.Items) error {
			sh.idCount++
			_, err := w.Write(sh.enc.Encode(batchID, items))
			return err
		},
	)
}

func (sh *SubHandler) OnClose(_ error) {
	// make sure handleAcks exits.
	close(sh.ackReset)
}

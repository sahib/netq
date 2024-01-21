package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/sahib/netq/protocol"
)

type CmdHandler struct {
	topics  *Topics
	wsBuf   bytes.Buffer
	copyBuf []byte
	cmdEnc  protocol.CmdEncoder
	writeCh chan []byte
}

func NewCmdHandler(topics *Topics) *CmdHandler {
	return &CmdHandler{
		topics:  topics,
		copyBuf: make([]byte, 16*1024),
		writeCh: make(chan []byte, 1),
	}
}

func (ch *CmdHandler) OnRead(ctx context.Context, r io.Reader) error {
	ch.wsBuf.Reset()
	if _, err := io.CopyBuffer(&ch.wsBuf, r, ch.copyBuf); err != nil {
		return err
	}

	id, cmd, payload, err := protocol.DecodeCmd(ch.wsBuf.Bytes())
	if err != nil {
		return err
	}

	resp, err := ch.handleCommand(cmd, payload)
	if err != nil {
		// NOTE: If command processing failed, we should not
		// terminate the connection.
		return nil
	}

	respData, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		return err
	}

	msgData := ch.cmdEnc.Encode(id, cmd, respData)
	select {
	case ch.writeCh <- msgData:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ch *CmdHandler) OnWrite(ctx context.Context, w io.Writer) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	case data := <-ch.writeCh:
		_, err := w.Write(data)
		return err
	}
}

func (ch *CmdHandler) handleCommand(cmd protocol.CmdType, data []byte) (any, error) {
	switch cmd {
	case protocol.CmdTypePing:
		// pong message has no request body nor response body.
		// the existence of the message is enough.
		return []byte{}, nil
	case protocol.CmdTypeTopicClear:
		var req protocol.CmdRequestTopicClear
		if err := json.Unmarshal(data, &req); err != nil {
			return nil, err
		}

		ts := TopicSpec(req.Topic)
		topic, err := ch.topics.Ref(ts)
		if err != nil {
			return nil, err
		}

		fork, err := topic.Fork(ts.ForkName())
		if err != nil {
			return nil, err
		}

		ncleared, err := fork.Clear()
		if err != nil {
			return nil, err
		}

		return protocol.CmdResponseTopicClear{
			Topic:    req.Topic,
			NCleared: ncleared,
		}, nil
	default:
		// this is probably a missing validation that should have been catched earlier.
		return nil, fmt.Errorf("invalid cmd type: %v", cmd)
	}
}

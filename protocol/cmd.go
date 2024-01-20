package protocol

import (
	"encoding/binary"
	"fmt"
)

type CmdType uint32

const (
	CmdTypePing = iota
	CmdTypeTopicClear
	CmdTypeTopicList
	cmdTypeMax
)

func (c CmdType) IsValid() bool {
	return c < cmdTypeMax
}

type CmdEncoder struct {
	buf []byte
}

func (ce *CmdEncoder) Encode(id uint64, cmd CmdType, payload []byte) []byte {
	var size uint32 = MessageHeaderSize + 4 + uint32(len(payload))
	if len(ce.buf) < int(size) {
		ce.buf = make([]byte, size)
	}

	EncodeHeader(ce.buf, Header{
		Type: TypeCmd,
		Size: size,
		ID:   id,
	})

	// Encode the type and payload:
	binary.BigEndian.PutUint32(ce.buf[MessageHeaderSize:], uint32(cmd))
	copy(ce.buf[MessageHeaderSize+4:], payload)
	return ce.buf
}

func DecodeCmd(data []byte) (uint64, CmdType, []byte, error) {
	header, err := DecodeHeader(data)
	if err != nil {
		return 0, 0, nil, err
	}

	if header.Type != TypeCmd {
		return 0, 0, nil, fmt.Errorf("not an cmd message: %v", header.Type)
	}

	const minSize = MessageHeaderSize + 4
	if header.Size < minSize {
		return 0, 0, nil, fmt.Errorf("cmd message is too short: %d", header.Size)
	}

	cmd := CmdType(binary.BigEndian.Uint32(data[MessageHeaderSize:]))
	if !cmd.IsValid() {
		return 0, 0, nil, fmt.Errorf("invalid cmd type: %d", cmd)
	}

	payload := data[MessageHeaderSize+4:]
	return header.ID, cmd, payload, nil
}

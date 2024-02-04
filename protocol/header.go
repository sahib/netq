package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	MessageHeaderSize = 16
	ItemHeaderSize    = 12

	MaxMessageSize = 64 * 1024 * 1024
	MaxItemSize    = 16 * 1024 * 1024
)

type Type uint32

const (
	TypeInvalid = iota
	TypeItems
	TypeError
	TypeAck
	TypeCmd
	typeMax
)

func (t Type) Validate() error {
	if t >= typeMax || t == TypeInvalid {
		return fmt.Errorf("invalid type: %v", t)
	}

	return nil
}

type Header struct {
	Type Type
	Size uint32
	ID   uint64
}

// DecodeHeader parses the header from `data`. It's okay to pass only $HeaderSize bytes
// of the full data package, as more is not needed.
func DecodeHeader(data []byte) (Header, error) {
	var header Header
	if l := len(data); l < MessageHeaderSize {
		return header, fmt.Errorf("header too small: %d", l)
	}

	header.Type = Type(binary.BigEndian.Uint32(data[0:4]))
	if err := header.Type.Validate(); err != nil {
		return header, err
	}

	header.Size = binary.BigEndian.Uint32(data[4:8])
	if header.Size > MaxMessageSize {
		return header, fmt.Errorf("message is bigger than %d: %v", MaxMessageSize, header.Size)
	}

	header.ID = binary.BigEndian.Uint64(data[8:16])
	return header, nil
}

func EncodeHeader(buf []byte, header Header) {
	binary.BigEndian.PutUint32(buf[0:], uint32(header.Type))
	binary.BigEndian.PutUint32(buf[4:], header.Size)
	binary.BigEndian.PutUint64(buf[8:], header.ID)
}

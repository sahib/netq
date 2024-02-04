package protocol

import (
	"fmt"
)

type AckEncoder struct {
	buf []byte
}

func (ae *AckEncoder) Encode(id uint64) []byte {
	var size uint32 = MessageHeaderSize
	if len(ae.buf) < int(size) {
		ae.buf = make([]byte, size)
	}

	EncodeHeader(ae.buf, Header{
		Type: TypeAck,
		Size: size,
		ID:   id,
	})
	return ae.buf
}

func DecodeAck(data []byte) (uint64, error) {
	header, err := DecodeHeader(data)
	if err != nil {
		return 0, err
	}

	if header.Type != TypeAck {
		return 0, fmt.Errorf("not an ack message: %v", header.Type)
	}

	return header.ID, nil
}

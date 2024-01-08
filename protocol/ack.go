package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/sahib/timeq"
	"github.com/sahib/timeq/item"
)

type AckEncoder struct {
	buf []byte
}

func (ae *AckEncoder) Encode(key timeq.Key) []byte {
	var size uint32 = MessageHeaderSize + 8
	if len(ae.buf) < int(size) {
		ae.buf = make([]byte, size)
	}

	EncodeHeader(ae.buf, Header{
		Type: TypeAck,
		Size: size,
	})

	binary.BigEndian.PutUint64(ae.buf[MessageHeaderSize:], uint64(key))
	return ae.buf
}

func DecodeAck(data []byte) (item.Key, error) {
	header, err := DecodeHeader(data)
	if err != nil {
		return 0, err
	}

	if header.Type != TypeAck {
		return 0, fmt.Errorf("not an ack message: %v", header.Type)
	}

	const expectedSize = MessageHeaderSize + 8
	if header.Size < expectedSize || len(data) < expectedSize {
		return 0, fmt.Errorf("message is too small: %d", header.Size)
	}

	// TODO: check size.
	key := binary.BigEndian.Uint64(data[MessageHeaderSize:])
	return timeq.Key(key), nil
}

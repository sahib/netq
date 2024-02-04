package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/sahib/timeq"
)

func DecodeBatch(data []byte) (uint64, timeq.Items, error) {
	var pos uint32 // just for logging

	header, err := DecodeHeader(data)
	if err != nil {
		return 0, nil, err
	}

	if header.Type != TypeItems {
		return 0, nil, fmt.Errorf("message has invalid type: %d", header.Type)
	}

	data = data[MessageHeaderSize:]
	if len(data) == 0 {
		return 0, nil, fmt.Errorf("empty items received")
	}

	// pre-allocate items by assuming that each item is 50 bytes in average:
	items := make(timeq.Items, 0, len(data)/(16+50))

	for len(data) > 0 {
		if len(data) < ItemHeaderSize {
			return 0, nil, fmt.Errorf("unexpected end of stream at pos %d", pos)
		}

		key := binary.BigEndian.Uint64(data[0:8])
		sze := binary.BigEndian.Uint32(data[8:12])

		if sze > MaxItemSize {
			return 0, nil, fmt.Errorf("individual item bigger than %d at pos %d", MaxItemSize, pos)
		}

		itemEnd := ItemHeaderSize + sze
		if len(data) < int(itemEnd) {
			return 0, nil, fmt.Errorf("unexpected end of payload at pos %d", pos)
		}

		items = append(items, timeq.Item{
			Key:  timeq.Key(key),
			Blob: data[ItemHeaderSize:itemEnd],
		})

		pos += itemEnd
		data = data[itemEnd:]
	}

	return header.ID, items, nil
}

type BatchEncoder struct {
	buf []byte
}

func (be *BatchEncoder) Encode(id uint64, items timeq.Items) []byte {
	size := MessageHeaderSize
	for idx := 0; idx < len(items); idx++ {
		size += len(items[idx].Blob) + ItemHeaderSize
	}

	// allocate just once in the correct size:
	if len(be.buf) < size {
		// need to allocate newly:
		be.buf = make([]byte, size)
	}

	EncodeHeader(be.buf, Header{
		ID:   id,
		Type: TypeItems,
		Size: uint32(size),
	})

	off := MessageHeaderSize
	for idx := 0; idx < len(items); idx++ {
		binary.BigEndian.PutUint64(be.buf[off+0:], uint64(items[idx].Key))
		binary.BigEndian.PutUint32(be.buf[off+8:], uint32(len(items[idx].Blob)))
		off += copy(be.buf[off+12:], items[idx].Blob)
		off += ItemHeaderSize
	}

	return be.buf
}

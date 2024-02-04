package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocolHeaderEncodeDecode(t *testing.T) {
	buf := make([]byte, MessageHeaderSize)
	exp := Header{
		Type: TypeItems,
		Size: 23,
		ID:   42,
	}

	EncodeHeader(buf, exp)
	got, err := DecodeHeader(buf)

	require.NoError(t, err)
	require.Equal(t, exp, got)
}

func TestProtocolDecodeHeaderFail(t *testing.T) {
	tcs := []struct {
		Name    string
		In      []byte
		ErrFrag string
	}{
		{
			Name:    "too_small",
			In:      make([]byte, MessageHeaderSize-1),
			ErrFrag: "too small",
		},
		{
			Name:    "invalid_type_zero",
			In:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			ErrFrag: "invalid type",
		},
		{
			Name:    "invalid_type_too_big",
			In:      []byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			ErrFrag: "invalid type",
		},
		{
			Name:    "too_big",
			In:      []byte{0x00, 0x00, 0x00, 0x01, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			ErrFrag: "bigger than",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := DecodeHeader(tc.In)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.ErrFrag)
		})
	}

}

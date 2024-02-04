package protocol

import (
	"fmt"
	"testing"

	"github.com/sahib/timeq/item/testutils"
	"github.com/stretchr/testify/require"
)

func TestProtocolBatch(t *testing.T) {
	enc := BatchEncoder{}

	expID := uint64(23)
	expItems := testutils.GenItems(0, 100, 1)

	data := enc.Encode(expID, expItems)
	fmt.Println(len(data))
	gotID, gotItems, err := DecodeBatch(data)

	require.NoError(t, err)
	require.Equal(t, expID, gotID)
	require.Equal(t, expItems, gotItems)
}

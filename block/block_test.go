package block

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockBuilder_Add(t *testing.T) {
	tests := []struct {
		giveBlockSize uint16
		wantErr       error
	}{
		{
			giveBlockSize: 100,
			wantErr:       nil,
		},
		{
			giveBlockSize: 1,
			wantErr:       ErrBlockFull,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("blockSize: %d", tt.giveBlockSize), func(t *testing.T) {
			bb := NewBlockBuilder(tt.giveBlockSize)
			err := bb.Add([]byte("key"), []byte("value"))
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func generateBlock(t *testing.T, blockSize uint16) *Block {
	bb := NewBlockBuilder(blockSize)
	tests := []struct {
		giveKey []byte
		giveVal []byte
		want    error
	}{
		{
			giveKey: []byte("key0"),
			giveVal: []byte("value0"),
			want:    nil,
		},
		{
			giveKey: []byte("key1"),
			giveVal: []byte("value1"),
			want:    nil,
		},
		{
			giveKey: []byte(nil),
			giveVal: []byte("value"),
			want:    ErrKeyEmpty,
		},
		{
			giveKey: []byte(""),
			giveVal: []byte("value"),
			want:    ErrKeyEmpty,
		},
	}
	for _, tt := range tests {
		t.Run(string(tt.giveKey)+":"+string(tt.giveVal), func(t *testing.T) {
			err := bb.Add(tt.giveKey, tt.giveVal)
			assert.Equal(t, tt.want, err)
		})
	}
	return bb.Build()
}

func TestBlock_Encode_Decode(t *testing.T) {
	b := generateBlock(t, 100)
	assert.NotNil(t, b)

	data := b.Encode()

	b2 := &Block{}
	b2.Decode(data)

	assert.Equal(t, b, b2)
}

func TestBlock_Iter_SeekToKey(t *testing.T) {
	b := generateBlock(t, 100)
	iter := NewBlockIter(b)
	err := iter.SeekToKey([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("key1"), iter.key)
	assert.Equal(t, []byte("value1"), iter.value)
}

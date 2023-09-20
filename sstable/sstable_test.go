package sstable

import (
	"fmt"
	"minilsm/block"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableBuilder_Add(t *testing.T) {
	tests := []struct {
		giveBlockSize uint16
		wantErr       error
	}{
		{
			giveBlockSize: 100,
			wantErr:       nil,
		},
		{
			giveBlockSize: 16,
			wantErr:       nil,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("blockSize: %d", tt.giveBlockSize), func(t *testing.T) {
			tb := NewTableBuilder(tt.giveBlockSize)
			err := tb.Add([]byte("key1"), []byte("value1"))
			assert.Equal(t, tt.wantErr, err)
			err = tb.Add([]byte("key1"), []byte("value1"))
			assert.Equal(t, tt.wantErr, err)
		})
	}

}

func generateTable(t *testing.T, blockSize uint16) *Table {
	tb := NewTableBuilder(blockSize)
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
			want:    block.ErrKeyEmpty,
		},
		{
			giveKey: []byte(""),
			giveVal: []byte("value"),
			want:    block.ErrKeyEmpty,
		},
	}
	for _, tt := range tests {
		t.Run(string(tt.giveKey)+":"+string(tt.giveVal), func(t *testing.T) {
			err := tb.Add(tt.giveKey, tt.giveVal)
			if err == nil {
				assert.Equal(t, tt.want, nil)
			} else {
				assert.ErrorAs(t, err, &tt.want)
			}
		})
	}
	sst, err := tb.Build(1, nil, "test.sst")
	assert.NoError(t, err)
	return sst
}

func TestSSTableBuilder(t *testing.T) {
	generateTable(t, 100)
	defer os.Remove("test.sst")
	contents, err := os.ReadFile("./test.sst")
	assert.NoError(t, err)
	t.Log(string(contents))
}

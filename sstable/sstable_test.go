package sstable

import (
	"bytes"
	"fmt"
	"minilsm/block"
	"minilsm/util"
	"os"
	"slices"
	"sync"
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

func TestSSTable_Build(t *testing.T) {
	tb := NewTableBuilder(1024)
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
	_, err := tb.Build(0, nil, "test.sst")
	defer os.Remove("test.sst")
	assert.NoError(t, err)
}

func generateSSTble(t *testing.T, pairs []struct {
	K []byte
	V []byte
}, blockSize uint16, path string) *Table {
	tb := NewTableBuilder(blockSize)
	for _, pair := range pairs {
		assert.NoError(t, tb.Add(pair.K, pair.V))
	}
	sst, err := tb.Build(1, &sync.Map{}, path)
	assert.NoError(t, err)
	return sst
}

func TestSSTable_Decode(t *testing.T) {
	pairs := util.GeneratePairs(1000)
	sst := generateSSTble(t, pairs, 1024, "test.sst")
	defer os.Remove("test.sst")
	defer sst.Close()

	nsst, err := openTableFromFile(1, &sync.Map{}, sst.fd)
	assert.NoError(t, err)
	assert.Equal(t, sst.metas, nsst.metas)
}

func TestSSTable_SeekToFirst(t *testing.T) {
	pairs := util.GeneratePairs(1000)
	sst := generateSSTble(t, pairs, 1024, "test.sst")
	defer os.Remove("test.sst")
	defer sst.Close()

	iter, err := NewIterAndSeekToFirst(sst)
	assert.NoError(t, err)
	for i := 0; i < 1000; i++ {
		assert.True(t, iter.IsValid())
		assert.Equal(t, pairs[i].K, iter.Key())
		assert.Equal(t, pairs[i].V, iter.Value())
		iter.Next()
	}
	assert.False(t, iter.IsValid())
}

func TestSSTable_SeekToKet(t *testing.T) {
	pairs := util.GeneratePairs(1000)
	slices.SortFunc(pairs, func(a, b struct {
		K []byte
		V []byte
	}) int {
		return bytes.Compare(a.K, b.K)
	})
	sst := generateSSTble(t, pairs, 1024, "test.sst")
	defer os.Remove("test.sst")
	defer sst.Close()

	for i := 0; i < 1000; i++ {
		iter, err := NewIterAndSeekToKey(sst, pairs[i].K)
		assert.NoError(t, err)
		assert.True(t, iter.IsValid())
		assert.Equal(t, pairs[i].K, iter.Key())
		assert.Equal(t, pairs[i].V, iter.Value())
	}
}

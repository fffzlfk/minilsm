package memtable

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemtable_Put_Get(t *testing.T) {
	tests := []struct {
		key     []byte
		value   []byte
		want    bool
		wantVal []byte
	}{
		{
			key:     []byte("key0"),
			value:   []byte("value0"),
			want:    true,
			wantVal: []byte("value0"),
		},
		{
			key:     []byte("key1"),
			value:   []byte("value1"),
			want:    true,
			wantVal: []byte("value1"),
		},
		{
			key:     []byte(""),
			value:   []byte("value"),
			want:    false,
			wantVal: nil,
		},
	}
	mt := NewTable()
	for _, tt := range tests {
		t.Run(string(tt.key)+":"+string(tt.value), func(t *testing.T) {
			ok := mt.Put(tt.key, tt.value)
			assert.Equal(t, tt.want, ok)
			val, ok := mt.Get(tt.key)
			assert.Equal(t, tt.wantVal, val)
			assert.Equal(t, tt.want, ok)
		})
	}
}

func TestMemtable_Iter(t *testing.T) {
	mt := NewTable()
	for i := 0; i < 10; i++ {
		mt.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	iter, err := mt.Scan([]byte("2"), []byte("5"))
	assert.NoError(t, err)

	assert.Equal(t, []byte("2"), iter.Key())
	iter.Next()
	assert.Equal(t, []byte("3"), iter.Key())
	iter.Next()
	assert.Equal(t, []byte("4"), iter.Key())
	iter.Next()
	assert.Equal(t, []byte("5"), iter.Key())
	iter.Next()
	assert.Nil(t, iter.Key())
}

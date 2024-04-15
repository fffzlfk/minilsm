package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList(t *testing.T) {
	sl := NewSkipList[int, int]()

	nums := []int{3, 6, 7, 9, 12, 19, 23, 25}
	for _, num := range nums {
		sl.Insert(num, num)
	}

	got, ok := sl.Search(6)
	assert.True(t, ok)
	assert.Equal(t, 6, got)

	_, ok = sl.Search(8)
	assert.False(t, ok)

	sl.Delete(6)
	sl.Delete(23)

	_, ok = sl.Search(6)
	assert.False(t, ok)
	_, ok = sl.Search(23)
	assert.False(t, ok)
}

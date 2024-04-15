package minilsm

import (
	"minilsm/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInternalStorage(t *testing.T) {
	path := t.TempDir()
	si := NewStorageInner(path)
	t.Cleanup(func() {
		for _, sst := range si.l0SSTables {
			sst.Close()
		}
	})

	kvs := util.GeneratePairs(1000)

	for _, kv := range kvs {
		ok := si.Put(kv.K, kv.V)
		assert.True(t, ok)
	}

	testRange(t, si, 500, 510)

	si.newMemTable()

	assert.NoError(t, si.sinkImmMemTableToSSTable())

	testRange(t, si, 500, 510)

	si.Put(util.KeyOf(400), util.ValueOf(0))
	si.Put(util.KeyOf(401), util.ValueOf(0))

	testRange(t, si, 500, 510)

	si.newMemTable()

	testRange(t, si, 500, 510)

	assert.NoError(t, si.sinkImmMemTableToSSTable())

	testRange(t, si, 500, 510)

	assert.NoError(t, si.compactSSTs())

	testRange(t, si, 500, 510)
}

func testRange(t *testing.T, si *StorageInner, from, to int) {
	scanner, err := si.Scan(util.KeyOf(from), util.KeyOf(to))
	assert.NoError(t, err)

	for i := from; i < to; i++ {
		assert.True(t, scanner.IsValid())
		assert.Equal(t, util.KeyOf(i), scanner.Key())
		assert.Equal(t, util.ValueOf(i), scanner.Value())
		scanner.Next()
	}
}

package minilsm

import (
	"math/rand"
	"minilsm/util"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInternalStorage(t *testing.T) {
	path := t.TempDir()
	si := NewStorageInner(path)
	t.Cleanup(func() {
		si.Close()
	})

	kvs := util.GeneratePairs(2000)

	for _, kv := range kvs {
		ok := si.Put(kv.K, kv.V)
		assert.True(t, ok)
	}

	testRange(t, si, 0, 2000)
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

func TestConcurrencySafe(t *testing.T) {
	path := t.TempDir()
	si := NewStorageInner(path)
	t.Cleanup(func() {
		si.Close()
	})

	var wg sync.WaitGroup
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			ok := si.Put(util.KeyOf(i), util.ValueOf(i))
			assert.True(t, ok)

			sleepDuration := rand.Intn(100-50+1) + 50 // sleep [50, 100] millisecond
			time.Sleep(time.Millisecond * time.Duration(sleepDuration))

			got, err := si.Get(util.KeyOf(i))
			assert.NoError(t, err)
			assert.Equal(t, util.ValueOf(i), got)
		}(i)
	}

	wg.Wait()
}

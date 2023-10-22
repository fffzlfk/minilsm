package memtable

import (
	"errors"
	"fmt"
	"minilsm/config"
	"minilsm/log"
	"minilsm/sstable"
	"minilsm/util"
	"sync"

	"github.com/huandu/skiplist"
)

type Table struct {
	mu sync.RWMutex
	sl *skiplist.SkipList
}

func NewTable() *Table {
	return &Table{
		sl: skiplist.New(skiplist.Bytes),
	}
}

func (t *Table) Get(key []byte) (val []byte, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(key) == 0 {
		log.Error("memtable get: key cannot be empty")
		return nil, false
	}
	v, ok := t.sl.GetValue(key)
	if !ok {
		return nil, false
	}
	val, ok = v.([]byte)
	if !ok {
		return nil, false
	}
	val = util.DeepCopySlice(val)
	return val, true
}

func (t *Table) Put(key, value []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(key) == 0 {
		log.Error("memtable put: key cannot be empty")
		return false
	}
	if len(key) > config.MaxKeyLength {
		log.Error("memtable put: key is too long")
		return false
	}
	t.sl.Set(util.DeepCopySlice(key), util.DeepCopySlice(value))
	return true
}

func (t *Table) Scan(lower, upper []byte) (*Iterator, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(lower) == 0 {
		return nil, errors.New("memtable scan: lower cannot be empty")
	}
	if len(upper) == 0 {
		return nil, errors.New("memtable scan: upper cannot be empty")
	}
	head := t.sl.Find(lower)
	return &Iterator{
		ele: head,
		end: upper,
	}, nil
}

func (t *Table) Flush(builder *sstable.TableBulder) error {
	head := t.sl.Front()
	if head == nil {
		return errors.New("memtable flush: table is empty")
	}
	for {
		err := builder.Add(head.Key().([]byte), head.Value.([]byte))
		if err != nil {
			return fmt.Errorf("memtable flush: %w", err)
		}
		next := head.Next()
		if next == nil {
			break
		}
		head = next
	}
	return nil
}

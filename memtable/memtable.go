package memtable

import (
	"errors"
	"fmt"
	"minilsm/config"
	"minilsm/logger"
	"minilsm/sstable"
	"minilsm/util"
	"sync"
)

var log = logger.GetLogger()

type Table struct {
	mu sync.RWMutex
	sl *SkipList[string, []byte]
}

func NewTable() *Table {
	return &Table{
		sl: NewSkipList[string, []byte](),
	}
}

func (t *Table) Get(key []byte) (val []byte, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(key) == 0 {
		log.Error("memtable get: key cannot be empty")
		return nil, false
	}
	val, ok = t.sl.Search(string(key))
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
	t.sl.Insert(string(key), util.DeepCopySlice(value))
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
	head, _ := t.sl.find(string(lower))
	return &Iterator{
		ele: head,
		end: upper,
	}, nil
}

func (t *Table) Flush(builder *sstable.TableBulder) error {
	head := t.sl.head
	if head == nil {
		return errors.New("memtable flush: table is empty")
	}
	current := head.forwards[0]
	if current == nil {
		return errors.New("memtable flush: table is empty")
	}

	for {
		err := builder.Add([]byte(current.key), current.value)
		if err != nil {
			return fmt.Errorf("memtable flush: %w", err)
		}
		next := current.forwards[0]
		if next == nil {
			break
		}
		current = next
	}
	return nil
}

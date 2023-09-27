package minilsm

import (
	"minilsm/memtable"
	"minilsm/sstable"
	"sync"
)

type StorageInner struct {
	mu sync.RWMutex

	memTableKeyCount uint64
	memTableSize     uint64
	memTable         *memtable.Table

	immMemTables []*memtable.Table

	l0SSTables []*sstable.Table
	levels     [][]*sstable.Table

	nextSSTableID uint32
	path          string
	blockCache    *sync.Map
}

func (si *StorageInner) Get(key []byte) []byte {
	si.mu.RLock()
	defer si.mu.RUnlock()
	val, ok := si.memTable.Get(key)
	if ok {
		return val
	}
	for _, imt := range si.immMemTables {
		val, ok := imt.Get(key)
		if ok {
			return val
		}
	}

	// TODO: get from sstable
	// iterators := make([]iterator.Iterator, 0, len(si.l0SSTables))
	// for _, t := range si.l0SSTables {
	// 	iterators = append(iterators)
	// }

	return nil
}

package minilsm

import (
	"bytes"
	"errors"
	"fmt"
	"minilsm/block"
	"minilsm/iterator"
	"minilsm/logger"
	"minilsm/memtable"
	"minilsm/sstable"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var log = logger.GetLogger()

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

	shouldClose chan struct{}
	isClosed    chan struct{}
}

func (si *StorageInner) Get(key []byte) ([]byte, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	val, ok := si.memTable.Get(key)
	if ok {
		return val, nil
	}
	for _, imt := range si.immMemTables {
		val, ok := imt.Get(key)
		if ok {
			return val, nil
		}
	}

	iterators := make([]iterator.Iterator, 0, len(si.l0SSTables))
	for _, t := range si.l0SSTables {
		iter, err := sstable.NewIterAndSeekToKey(t, key)
		if err != nil {
			if errors.Is(err, block.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("scan: %w", err)
		}
		iterators = append(iterators, iter)
	}

	mergedIter := iterator.NewMergeIterator(iterators...)
	if mergedIter.IsValid() && bytes.Equal(key, mergedIter.Key()) {
		return mergedIter.Value(), nil
	}

	return nil, errors.New("get: key not found")
}

func (si *StorageInner) Put(key, value []byte) bool {
	si.mu.RLock()
	ok := si.memTable.Put(key, value)
	si.mu.RUnlock()
	if ok {
		atomic.AddUint64(&si.memTableKeyCount, 1)
		estimateSize := block.SizeOfUint16*2 + len(key) + len(value) + block.SizeOfUint16
		atomic.AddUint64(&si.memTableSize, uint64(estimateSize))
	}
	return ok
}

func (si *StorageInner) Del(key []byte) bool {
	return si.memTable.Put(key, nil)
}

func (si *StorageInner) Scan(lower, upper []byte) (iterator.Iterator, error) {
	iters := make([]iterator.Iterator, 0, 1+len(si.immMemTables)+len(si.l0SSTables))
	iter, err := si.memTable.Scan(lower, upper)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	iters = append(iters, iter)
	for _, t := range si.immMemTables {
		iter, err := t.Scan(lower, upper)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		iters = append(iters, iter)
	}
	for _, t := range si.l0SSTables {
		iter, err := sstable.NewIterAndSeekToKey(t, lower)
		if err != nil {
			if errors.Is(err, block.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("scan: %w", err)
		}
		iters = append(iters, iter)
	}
	return iterator.NewMergeIterator(iters...), nil
}

func (si *StorageInner) checkIfNewMemTableShouldBeCreate() bool {
	return atomic.LoadUint64(&si.memTableKeyCount) >= 1000 || atomic.LoadUint64(&si.memTableSize) >= 10*4*1024
}

func (si *StorageInner) newMemTable() {
	si.mu.Lock()
	si.memTable, si.immMemTables = memtable.NewTable(), append(si.immMemTables, si.memTable)
	si.mu.Unlock()

	atomic.SwapUint64(&si.memTableKeyCount, 0)
	atomic.SwapUint64(&si.memTableSize, 0)
}

func (si *StorageInner) checkIfImmMemTableShouldFlushToSSTable() bool {
	si.mu.RLock()
	defer si.mu.RUnlock()

	return len(si.immMemTables) > 0
}

func (si *StorageInner) sstPath(id uint32) string {
	return filepath.Join(si.path, strconv.Itoa(int(id))+".sst")
}

func (si *StorageInner) sinkImmMemTableToSSTable() error {
	si.mu.Lock()
	defer si.mu.Unlock()

	if len(si.immMemTables) == 0 {
		return nil
	}

	sstID := si.nextSSTableID
	flushMemTable := si.immMemTables[len(si.immMemTables)-1]
	builder := sstable.NewTableBuilder(4096)
	err := flushMemTable.Flush(builder)
	if err != nil {
		return fmt.Errorf("sinkImmMemTableToSSTable: %w", err)
	}

	ssTable, err := builder.Build(sstID, si.blockCache, si.sstPath(sstID))
	if err != nil {
		return fmt.Errorf("sinkImmMemTableToSSTable: %w", err)
	}

	si.immMemTables = si.immMemTables[:len(si.immMemTables)-1]
	si.l0SSTables = append([]*sstable.Table{ssTable}, si.l0SSTables...)
	si.nextSSTableID += 1

	return nil
}

func (si *StorageInner) checkIfSSTShouldBeCompact() bool {
	si.mu.RLock()
	defer si.mu.RUnlock()

	return len(si.l0SSTables) >= 2
}

func (si *StorageInner) compactSSTs() error {
	log.Infof("compact with l0SSTables: %v", len(si.l0SSTables))
	if len(si.l0SSTables) >= 2 {
		si.mu.RLock()
		l0SSTableLength := len(si.l0SSTables)
		sn := si.l0SSTables[l0SSTableLength-1]
		snID := sn.SSTID()
		snm1 := si.l0SSTables[l0SSTableLength-2]
		snm1ID := snm1.SSTID()
		si.mu.RUnlock()

		snIter, error := sstable.NewIterAndSeekToFirst(sn)
		if error != nil {
			return fmt.Errorf("compact: %w", error)
		}
		snm1Iter, error := sstable.NewIterAndSeekToFirst(snm1)
		if error != nil {
			return fmt.Errorf("compact: %w", error)
		}

		mergedIter := iterator.NewMergeIterator(snIter, snm1Iter)
		builder := sstable.NewTableBuilder(4096)
		for mergedIter.IsValid() {
			builder.Add(mergedIter.Key(), mergedIter.Value())
			mergedIter.Next()
		}

		sstID := si.nextSSTableID
		ssTable, err := builder.Build(sstID, si.blockCache, si.sstPath(sstID))
		if err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		si.nextSSTableID += 1

		defer func() {
			sn.Close()
			snm1.Close()
			os.Remove(si.sstPath(snID))
			os.Remove(si.sstPath(snm1ID))
		}()

		si.mu.Lock()
		if si.l0SSTables[l0SSTableLength-1].SSTID() == snID && si.l0SSTables[l0SSTableLength-2].SSTID() == snm1ID {
			si.l0SSTables = append(si.l0SSTables[0:l0SSTableLength-2], ssTable)
		}
		si.mu.Unlock()
	}
	return nil
}

func (si *StorageInner) internalLoopTask() {
	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		if si.checkIfNewMemTableShouldBeCreate() {
			log.Info("create new memtable\n")
			si.newMemTable()
		}

		if si.checkIfImmMemTableShouldFlushToSSTable() {
			log.Info("start to sink immutable memtable to sstable\n")
			err := si.sinkImmMemTableToSSTable()
			if err != nil {
				log.Errorf("internalLoopTask: ", err)
			}
		}

		if si.checkIfSSTShouldBeCompact() {
			si.compactSSTs()
		}

		select {
		case <-si.shouldClose:
			for _, sst := range si.l0SSTables {
				sst.Close()
			}
			ticker.Stop()
			si.isClosed <- struct{}{}
			return
		default:
			continue
		}
	}
}

func (si *StorageInner) Close() {
	si.shouldClose <- struct{}{}
	<-si.isClosed
}

func NewStorageInner(path string) *StorageInner {
	si := &StorageInner{
		memTable:      memtable.NewTable(),
		immMemTables:  make([]*memtable.Table, 0),
		l0SSTables:    make([]*sstable.Table, 0),
		levels:        make([][]*sstable.Table, 0),
		nextSSTableID: 1,
		path:          path,
		blockCache:    &sync.Map{},
		shouldClose:   make(chan struct{}, 1),
		isClosed:      make(chan struct{}),
	}
	go si.internalLoopTask()
	return si
}

package sstable

import (
	"fmt"
	"minilsm/block"
	"minilsm/iterator"
	"minilsm/log"
)

type Iter struct {
	table     *Table
	blockIter *block.Iter
	blockIdx  uint32
}

// IsValid implements iterator.Iterator.
func (i *Iter) IsValid() bool {
	return i.blockIter.IsValid()
}

// Key implements iterator.Iterator.
func (i *Iter) Key() []byte {
	return i.blockIter.Key()
}

// Next implements iterator.Iterator.
func (i *Iter) Next() {
	i.blockIter.Next()
	if !i.blockIter.IsValid() {
		i.blockIdx++
		if i.blockIdx < i.table.Len() {
			blk, err := i.table.ReadBlockCached(i.blockIdx)
			if err != nil {
				log.Error("next: %v", err)
			}
			iter, err := block.NewBlockIterAndSeekToFirst(blk)
			if err != nil {
				log.Error("next: %v", err)
			}
			i.blockIter = iter
		}
	}
}

// Value implements iterator.Iterator.
func (i *Iter) Value() []byte {
	return i.blockIter.Value()
}

var _ iterator.Iterator = (*Iter)(nil)

func NewIterAndSeekToFirst(table *Table) (*Iter, error) {
	blk, err := table.ReadBlockCached(0)
	if err != nil {
		return nil, fmt.Errorf("read block: %w", err)
	}
	blkIter, err := block.NewBlockIterAndSeekToFirst(blk)
	if err != nil {
		return nil, fmt.Errorf("new block iter and seek to first: %w", err)
	}
	return &Iter{
		table:     table,
		blockIter: blkIter,
		blockIdx:  0,
	}, nil
}

func NewIterAndSeekToKey(table *Table, key []byte) (*Iter, error) {
	blkIdx, iter, err := seekToKey(table, key)
	if err != nil {
		return nil, fmt.Errorf("new sstable iter and seek to key: %w", err)
	}
	return &Iter{
		table:     table,
		blockIter: iter,
		blockIdx:  blkIdx,
	}, nil
}

func seekToKey(t *Table, key []byte) (uint32, *block.Iter, error) {
	blkIdx := t.FindBlockIdx(key)
	blk, err := t.ReadBlockCached(blkIdx)
	if err != nil {
		return 0, nil, fmt.Errorf("seek to key: %w", err)
	}

	blkIter, err := block.NewBlockIterAndSeekToKey(blk, key)
	if err != nil {
		return 0, nil, fmt.Errorf("seek to key: %w", err)
	}

	return blkIdx, blkIter, nil
}

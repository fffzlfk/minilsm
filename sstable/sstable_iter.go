package sstable

import (
	"fmt"
	"minilsm/block"
	"minilsm/iterator"
)

type Iter struct {
	table     *Table
	blockIter *block.Iter
	blockIdx  uint32
}

// IsValid implements iterator.Iterator.
func (*Iter) IsValid() bool {
	panic("unimplemented")
}

// Key implements iterator.Iterator.
func (*Iter) Key() []byte {
	panic("unimplemented")
}

// Next implements iterator.Iterator.
func (*Iter) Next() {
	panic("unimplemented")
}

// Value implements iterator.Iterator.
func (*Iter) Value() []byte {
	panic("unimplemented")
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
		return nil, fmt.Errorf("new block iter and seek to key: %w", err)
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

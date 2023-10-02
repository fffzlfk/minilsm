package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type Iter struct {
	block *Block
	key   []byte
	value []byte
	idx   int
}

func (i *Iter) Key() []byte {
	return i.key
}

func (i *Iter) Value() []byte {
	return i.value
}

func (i *Iter) IsValid() bool {
	return i != nil && i.block != nil && len(i.key) > 0
}

func (i *Iter) Next() {
	if i.block != nil {
		return
	}
	i.idx++
	i.seekTo(i.idx)
}

func NewBlockIter(block *Block) *Iter {
	return &Iter{
		block: block,
		idx:   0,
	}
}

func NewBlockIterAndSeekToFirst(block *Block) (*Iter, error) {
	iter := NewBlockIter(block)
	if err := iter.seekTo(0); err != nil {
		return nil, fmt.Errorf("new block iter and seek to first: %w", err)
	}
	return iter, nil
}

func NewBlockIterAndSeekToKey(block *Block, key []byte) (*Iter, error) {
	iter := NewBlockIter(block)
	if err := iter.SeekToKey(key); err != nil {
		return nil, fmt.Errorf("new block iter and seek to key: %w", err)
	}
	return iter, nil
}

func (i *Iter) seekTo(index int) error {
	if index >= len(i.block.offsets) {
		return errors.New("seek to invalid index")
	}
	offset := i.block.offsets[index]
	i.seekToOffset(offset)
	return nil
}

func (i *Iter) seekToOffset(offset uint16) error {
	if int(offset) >= len(i.block.data) {
		return errors.New("seek to invalid offset")
	}
	entry := i.block.data[offset:]
	ks := binary.LittleEndian.Uint16(entry[:sizeOfUint16])
	key := make([]byte, ks)
	copy(key, entry[sizeOfUint16:])
	i.key = key
	vs := binary.LittleEndian.Uint16(entry[sizeOfUint16+ks:])
	value := make([]byte, vs)
	copy(value, entry[sizeOfUint16+ks+sizeOfUint16:])
	i.value = value
	return nil
}

func (i *Iter) SeekToKey(key []byte) error {
	if len(key) <= 0 {
		return errors.New("seek to key: empty key")
	}
	l, r := 0, len(i.block.offsets)-1
	for l < r {
		mid := l + (r-l+1)/2
		if err := i.seekTo(mid); err != nil {
			return fmt.Errorf("seek to key: %w", err)
		}
		if bytes.Compare(key, i.key) < 0 {
			r = mid - 1
		} else {
			l = mid
		}
	}
	if err := i.seekTo(l); err != nil {
		return fmt.Errorf("seek to key: %w", err)
	}
	return nil
}

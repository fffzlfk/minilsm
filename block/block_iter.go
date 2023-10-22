package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"minilsm/log"
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
	return i != nil && i.block != nil && len(i.key) > 0 && i.idx < len(i.block.offsets)
}

func (i *Iter) Next() {
	if i.block == nil {
		return
	}
	i.idx++
	if err := i.seekTo(i.idx); err != nil {
		log.Info("block iter next: %v", err)
		return
	}
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

var ErrKeyNotFound = errors.New("key not found")

func (i *Iter) seekTo(index int) error {
	if index >= len(i.block.offsets) {
		log.Info("seek to: invalid index")
		return errors.New("seek to: invalid index")
	}
	offset := i.block.offsets[index]
	i.seekToOffset(offset)
	i.idx = index
	return nil
}

func (i *Iter) seekToOffset(offset uint16) error {
	if int(offset) >= len(i.block.data) {
		return errors.New("seek to invalid offset")
	}
	entry := i.block.data[offset:]
	ks := binary.LittleEndian.Uint16(entry[:SizeOfUint16])
	key := make([]byte, ks)
	copy(key, entry[SizeOfUint16:])
	i.key = key
	vs := binary.LittleEndian.Uint16(entry[SizeOfUint16+ks:])
	value := make([]byte, vs)
	copy(value, entry[SizeOfUint16+ks+SizeOfUint16:])
	i.value = value
	return nil
}

func (i *Iter) SeekToKey(key []byte) error {
	if len(key) <= 0 {
		return errors.New("seek to key: empty key")
	}
	l, r := 0, len(i.block.offsets)
	for l < r {
		mid := l + (r-l)/2
		if err := i.seekTo(mid); err != nil {
			return fmt.Errorf("2 seek to key: %w", err)
		}
		switch bytes.Compare(i.key, key) {
		case -1:
			l = mid + 1
		case 0:
			return nil
		case 1:
			r = mid
		}
	}
	if err := i.seekTo(l); err != nil {
		return fmt.Errorf("seek to key: %w", ErrKeyNotFound)
	}
	return nil
}

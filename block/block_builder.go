package block

import (
	"encoding/binary"
	"errors"
	"minilsm/config"
)

const (
	SizeOfUint16 = 2
)

type Builder struct {
	offsets    []uint16
	data       []byte
	dataCursor uint16
	blockSize  uint16
}

func NewBlockBuilder(size uint16) *Builder {
	return &Builder{
		offsets:    make([]uint16, 0),
		data:       make([]byte, size),
		dataCursor: 0,
		blockSize:  size,
	}
}

func estimateGrow(key, value []byte) uint16 {
	return uint16(len(key)) + uint16(len(value)) + SizeOfUint16*2 + SizeOfUint16 // kLen | key | vLen | value | offset
}

func (b *Builder) IsEmpty() bool {
	return len(b.offsets) == 0
}

var (
	ErrKeyEmpty   = errors.New("key is empty")
	ErrKeyTooLong = errors.New("key is too long")
	ErrBlockFull  = errors.New("block is full")
)

// +----------+-------+------------+-------+
// | key size |  key  | value size | value |
// +----------+-------+------------+-------+
// | uint16   | bytes |  uint16    | bytes |
// +----------+-------+------------+-------+
func (b *Builder) Add(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > config.MaxKeyLength {
		return ErrKeyTooLong
	}

	if b.dataCursor+estimateGrow(key, value) > b.blockSize {
		return ErrBlockFull
	}

	b.offsets = append(b.offsets, b.dataCursor)

	binary.LittleEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+SizeOfUint16], uint16(len(key)))
	b.dataCursor += SizeOfUint16
	b.dataCursor += uint16(copy(b.data[b.dataCursor:], key))
	binary.LittleEndian.PutUint16(b.data[b.dataCursor:b.dataCursor+SizeOfUint16], uint16(len(value)))
	b.dataCursor += SizeOfUint16
	b.dataCursor += uint16(copy(b.data[b.dataCursor:], value))

	return nil
}

func (b *Builder) Build() *Block {
	return &Block{
		data:    b.data,
		offsets: b.offsets,
	}
}

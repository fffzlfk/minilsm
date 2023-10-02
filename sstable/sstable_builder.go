package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"minilsm/block"
	"minilsm/log"
	"minilsm/util"
	"os"
	"sync"
)

type TableBulder struct {
	builder   *block.Builder
	firstKey  []byte
	data      [][]byte
	dataSize  uint32
	metas     []*block.Meta
	blockSize uint16
}

func NewTableBuilder(blockSize uint16) *TableBulder {
	return &TableBulder{
		builder:   block.NewBlockBuilder(blockSize),
		metas:     make([]*block.Meta, 0),
		blockSize: blockSize,
	}
}

func (tb *TableBulder) Add(key, value []byte) (err error) {
	if tb.firstKey == nil {
		tb.firstKey = util.DeepCopySlice(key)
	}
	err = tb.builder.Add(key, value)
	if err != nil {
		if errors.Is(err, block.ErrBlockFull) {
			tb.finishBlock()
			if tb.Add(key, value) != nil {
				log.Fatal("tablebuilder add: %v", err)
			}
		} else {
			return fmt.Errorf("tablebuilder add: %w", err)
		}
	}
	return nil
}

func (tb *TableBulder) finishBlock() {
	if !tb.builder.IsEmpty() {
		tb.metas = append(tb.metas, block.NewBlockMeta(tb.dataSize, tb.firstKey))
		data := tb.builder.Build().Encode()
		tb.data = append(tb.data, data)
		tb.dataSize += uint32(len(data))
	}
	tb.builder = block.NewBlockBuilder(tb.blockSize)
}

var errIntenalWriteError = errors.New("internal write error")

var errBuildInternalWriteError = fmt.Errorf("tablebuilder build: %w", errIntenalWriteError)

func (tb *TableBulder) Build(id uint32, cache *sync.Map, path string) (*Table, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("tablebuilder build: %w", err)
	}
	tb.finishBlock()

	for i := range tb.data {
		n, err := fd.Write(tb.data[i])
		if n != len(tb.data[i]) {
			return nil, errBuildInternalWriteError
		}
		if err != nil {
			return nil, fmt.Errorf("tablebuilder build: %w", err)
		}
	}

	metaData := block.EncodeBlockMeta(tb.metas)
	n, err := fd.Write(metaData[:])
	if n != len(metaData) {
		return nil, errBuildInternalWriteError
	}
	if err != nil {
		return nil, fmt.Errorf("tablebuilder build: %w", err)
	}

	var buf [block.SizeOfUint32]byte
	binary.LittleEndian.PutUint32(buf[:], tb.dataSize)
	n, err = fd.Write(buf[:])
	if n != block.SizeOfUint32 {
		return nil, errBuildInternalWriteError
	}
	if err != nil {
		return nil, fmt.Errorf("tablebuilder build: %w", err)
	}

	err = fd.Sync()
	if err != nil {
		return nil, fmt.Errorf("tablebuilder build: %w", err)
	}

	return &Table{
		id:          id,
		fd:          fd,
		metas:       tb.metas,
		metasOffset: tb.dataSize,
		blockCache:  cache,
	}, nil
}

package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"minilsm/block"
	"os"
	"sync"
)

type Table struct {
	id          uint32
	fd          *os.File
	metas       []*block.Meta
	metasOffset uint32
	blockCache  *sync.Map
}

// | ...blocks... | blocks_meta | blocks_meta_offset |
func openTableFromFile(id uint32, blockCache *sync.Map, fd *os.File) (*Table, error) {
	errorHandle := func(e error, n int, got int) error {
		if e != nil {
			return fmt.Errorf("open table file failed: %v", e)
		}
		if n != got {
			return errors.New("invalid meta offset")
		}
		return nil
	}

	fi, err := fd.Stat()
	if err := errorHandle(err, 0, 0); err != nil {
		return nil, err
	}

	var raw [block.SizeOfUint32]byte
	n, err := fd.ReadAt(raw[:], fi.Size()-block.SizeOfUint32)
	if err := errorHandle(err, n, block.SizeOfUint32); err != nil {
		return nil, err
	}
	blockMetaOffset := binary.LittleEndian.Uint32(raw[:])

	_, err = fd.Seek(int64(blockMetaOffset), io.SeekStart)
	if err := errorHandle(err, 0, 0); err != nil {
		return nil, err
	}

	metas, err := block.DecodeBlockMetaFromReader(io.LimitReader(fd, int64(fi.Size())-int64(block.SizeOfUint32)-int64(blockMetaOffset)))
	if err := errorHandle(err, 0, 0); err != nil {
		return nil, err
	}

	return &Table{
		id:          id,
		fd:          fd,
		metas:       metas,
		metasOffset: blockMetaOffset,
		blockCache:  blockCache,
	}, nil
}

func (t *Table) Close() error {
	err := t.fd.Close()
	if err != nil {
		return fmt.Errorf("table close: %w", err)
	}
	return nil
}

func (t *Table) ReadBlock(blockIdx uint32) (*block.Block, error) {
	offset := t.metas[blockIdx].Offset
	var nextOffset uint32
	if blockIdx < uint32(len(t.metas)-1) {
		nextOffset = t.metas[blockIdx+1].Offset
	} else {
		nextOffset = t.metasOffset
	}
	buf := make([]byte, nextOffset-offset)
	n, err := t.fd.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("table read block: %w", err)
	}
	if n != len(buf) {
		return nil, errors.New("read block data failed")
	}
	var b block.Block
	if err := b.Decode(buf); err != nil {
		return nil, fmt.Errorf("table read block: %w", err)
	}
	return &b, nil
}

func (t *Table) ReadBlockCached(blockIdx uint32) (*block.Block, error) {
	key := [2]uint32{t.id, blockIdx}
	if v, ok := t.blockCache.Load(key); ok {
		return v.(*block.Block), nil
	}
	b, err := t.ReadBlock(blockIdx)
	if err != nil {
		return nil, err
	}
	t.blockCache.Store(key, b)
	return b, nil
}

func (t *Table) Len() uint32 {
	return uint32(len(t.metas))
}

func (t *Table) FindBlockIdx(key []byte) uint32 {
	for i := uint32(0); i < t.Len(); i++ {
		if bytes.Compare(t.metas[i].FirstKey, key) > 0 {
			return i - 1
		}
	}
	return t.Len() - 1
}

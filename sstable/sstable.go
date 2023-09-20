package sstable

import (
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

package memtable

import (
	"bytes"
	"minilsm/util"

	"github.com/huandu/skiplist"
)

type Iterator struct {
	ele *skiplist.Element
	end []byte
}

func (i *Iterator) Value() []byte {
	return util.DeepCopySlice(i.ele.Value.([]byte))
}

func (i *Iterator) Key() []byte {
	if i.ele == nil {
		return nil
	}
	return util.DeepCopySlice(i.ele.Key().([]byte))
}

func (i *Iterator) IsValid() bool {
	return i.ele != nil && len(i.ele.Key().([]byte)) != 0
}

func (i *Iterator) Next() {
	i.ele = i.ele.Next()
	if i.ele != nil && bytes.Compare(i.ele.Key().([]byte), i.end) > 0 {
		i.ele = nil
	}
}

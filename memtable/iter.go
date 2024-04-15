package memtable

import (
	"bytes"
	"minilsm/util"
)

type Iterator struct {
	ele *Node[string, []byte]
	end []byte
}

func (i *Iterator) Value() []byte {
	return util.DeepCopySlice(i.ele.value)
}

func (i *Iterator) Key() []byte {
	if i.ele == nil {
		return nil
	}
	return []byte(i.ele.key)
}

func (i *Iterator) IsValid() bool {
	return i.ele != nil && len(i.ele.key) != 0
}

func (i *Iterator) Next() {
	i.ele = i.ele.forwards[0]
	if i.ele != nil && bytes.Compare([]byte(i.ele.key), i.end) > 0 {
		i.ele = nil
	}
}

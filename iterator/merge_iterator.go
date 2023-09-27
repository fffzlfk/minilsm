package iterator

import (
	"bytes"
	"minilsm/util"
)

type MergeIterator struct {
	iterators []Iterator
	currrent  int
}

func NewMergeIterator(in ...Iterator) *MergeIterator {
	if len(in) == 0 {
		return &MergeIterator{
			iterators: in,
			currrent:  -1,
		}
	}
	iterators := make([]Iterator, 0)
	for _, in := range in {
		if in.IsValid() {
			iterators = append(iterators, in)
		}
	}
	if len(iterators) == 0 {
		return &MergeIterator{
			iterators: in,
			currrent:  -1,
		}
	}
	return &MergeIterator{
		iterators: iterators,
		currrent:  minIter(iterators),
	}
}

func minIter(iterators []Iterator) int {
	min := 0
	for i, it := range iterators {
		if bytes.Compare(it.Key(), iterators[min].Key()) < 0 {
			min = i
		}
	}
	return min
}

func (m *MergeIterator) currentIter() Iterator {
	return m.iterators[m.currrent]
}

func (m *MergeIterator) Key() []byte {
	return m.currentIter().Key()
}

func (m *MergeIterator) Value() []byte {
	return m.currentIter().Value()
}

func (m *MergeIterator) IsValid() bool {
	return m.currrent >= 0 && m.currrent < len(m.iterators) && m.currentIter().IsValid()
}

// Next skip all same key in iters
func (m *MergeIterator) Next() {
	currentKey := util.DeepCopySlice(m.Key())

	m.currentIter().Next()
	if !m.currentIter().IsValid() { // currrent iter has no elements
		m.iterators = append(m.iterators[:m.currrent], m.iterators[m.currrent+1:]...)
	}

	// remove all duplicate keys
	for i := 0; i < len(m.iterators); i++ {
		for m.iterators[i].IsValid() && bytes.Equal(m.iterators[i].Key(), currentKey) {
			m.iterators[i].Next()
		}
	}

	// remove invalid iter
	i := len(m.iterators) - 1
	for i >= 0 {
		if !m.iterators[i].IsValid() {
			m.iterators = append(m.iterators[:i], m.iterators[i+1:]...)
		}
		i--
	}

	m.currrent = minIter(m.iterators)
}

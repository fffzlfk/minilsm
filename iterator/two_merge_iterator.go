package iterator

import (
	"bytes"
)

type TwoMergeIterator struct {
	A       Iterator
	B       Iterator
	chooseA bool
}

func NewTwoMerger(a, b Iterator) *TwoMergeIterator {
	iter := &TwoMergeIterator{
		A: a,
		B: b,
	}
	iter.skipB()
	iter.choose()
	return iter
}

func (t *TwoMergeIterator) skipB() {
	if t.A.IsValid() {
		for t.B.IsValid() && bytes.Equal(t.A.Key(), t.B.Key()) {
			t.B.Next()
		}
	}
}

func (t *TwoMergeIterator) choose() {
	if !t.A.IsValid() {
		t.chooseA = false
		return
	}
	if !t.B.IsValid() {
		t.chooseA = true
		return
	}
	t.chooseA = bytes.Compare(t.A.Key(), t.B.Key()) < 0
}

func (t *TwoMergeIterator) Key() []byte {
	if t.chooseA {
		return t.A.Key()
	}
	return t.B.Key()
}

func (t *TwoMergeIterator) Value() []byte {
	if t.chooseA {
		return t.A.Value()
	}
	return t.B.Value()
}

func (t *TwoMergeIterator) IsValid() bool {
	if t.chooseA {
		return t.A.IsValid()
	}
	return t.B.IsValid()
}

func (t *TwoMergeIterator) Next() {
	if t.chooseA {
		t.A.Next()
	} else {
		t.B.Next()
	}
	t.skipB()
	t.choose()
}

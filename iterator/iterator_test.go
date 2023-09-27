package iterator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockIterator struct {
	Data []struct {
		K []byte
		V []byte
	}
	Index int
}

func newMockIterator(data []struct{ K, V []byte }) *mockIterator {
	return &mockIterator{
		Data:  data,
		Index: 0,
	}
}

func (m *mockIterator) Key() []byte {
	return m.Data[m.Index].K
}

func (m *mockIterator) Value() []byte {
	return m.Data[m.Index].V
}

func (m *mockIterator) Next() {
	m.Index += 1
}

func (m *mockIterator) IsValid() bool {
	return m.Index < len(m.Data)
}

var _ Iterator = (*mockIterator)(nil)

func checkIterResult(t *testing.T, iter Iterator, expected []struct{ K, V []byte }) {
	for i := 0; i < len(expected); i++ {
		assert.True(t, iter.IsValid())
		assert.Equal(t, expected[i].K, iter.Key())
		assert.Equal(t, expected[i].V, iter.Value())
		iter.Next()
	}
	assert.False(t, iter.IsValid())
}

func TestTwoMerge(t *testing.T) {
	t.Run("TwoMerge1", func(t *testing.T) {
		a := newMockIterator([]struct{ K, V []byte }{
			{[]byte("1"), []byte("1.a")},
			{[]byte("2"), []byte("2.a")},
			{[]byte("3"), []byte("3.a")},
		})
		b := newMockIterator([]struct{ K, V []byte }{
			{[]byte("1"), []byte("1.b")},
			{[]byte("2"), []byte("2.b")},
			{[]byte("3"), []byte("3.b")},
			{[]byte("4"), []byte("4.b")},
		})
		tmIter := NewTwoMerger(a, b)
		checkIterResult(t, tmIter, []struct{ K, V []byte }{
			{[]byte("1"), []byte("1.a")},
			{[]byte("2"), []byte("2.a")},
			{[]byte("3"), []byte("3.a")},
			{[]byte("4"), []byte("4.b")},
		})
	})
	t.Run("TwoMerge2", func(t *testing.T) {
		a := newMockIterator([]struct{ K, V []byte }{
			{[]byte("1"), []byte("1.a")},
			{[]byte("2"), []byte("2.a")},
			{[]byte("3"), []byte("3.a")},
			{[]byte("5"), []byte("5.a")},
		})
		b := newMockIterator([]struct{ K, V []byte }{
			{[]byte("1"), []byte("1.b")},
			{[]byte("2"), []byte("2.b")},
			{[]byte("3"), []byte("3.b")},
			{[]byte("4"), []byte("4.b")},
		})
		tmIter := NewTwoMerger(a, b)
		checkIterResult(t, tmIter, []struct{ K, V []byte }{
			{[]byte("1"), []byte("1.a")},
			{[]byte("2"), []byte("2.a")},
			{[]byte("3"), []byte("3.a")},
			{[]byte("4"), []byte("4.b")},
			{[]byte("5"), []byte("5.a")},
		})
	})
}

func TestMerge(t *testing.T) {
	i1 := newMockIterator([]struct{ K, V []byte }{
		{[]byte("1"), []byte("1.a")},
		{[]byte("2"), []byte("2.a")},
		{[]byte("3"), []byte("3.a")},
	})
	i2 := newMockIterator([]struct{ K, V []byte }{
		{[]byte("1"), []byte("1.b")},
		{[]byte("2"), []byte("2.b")},
		{[]byte("3"), []byte("3.b")},
		{[]byte("4"), []byte("4.b")},
	})
	i3 := newMockIterator([]struct{ K, V []byte }{
		{[]byte("2"), []byte("2.c")},
		{[]byte("3"), []byte("3.c")},
		{[]byte("4"), []byte("4.c")},
	})
	t.Run("MergeIterator1", func(t *testing.T) {
		checkIterResult(t, NewMergeIterator(i1, i2, i3), []struct{ K, V []byte }{
			{[]byte("1"), []byte("1.a")},
			{[]byte("2"), []byte("2.a")},
			{[]byte("3"), []byte("3.a")},
			{[]byte("4"), []byte("4.b")},
		})
		t.Cleanup(func() {
			i1.Index = 0
			i2.Index = 0
			i3.Index = 0
		})
	})
	t.Run("MergeIterator2", func(t *testing.T) {
		checkIterResult(t, NewMergeIterator(i3, i2, i1), []struct{ K, V []byte }{
			{[]byte("1"), []byte("1.b")},
			{[]byte("2"), []byte("2.c")},
			{[]byte("3"), []byte("3.c")},
			{[]byte("4"), []byte("4.c")},
		})
	})
}

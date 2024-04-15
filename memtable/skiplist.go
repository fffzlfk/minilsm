package memtable

import (
	"cmp"
	"math/rand"
)

type Node[K cmp.Ordered, V any] struct {
	key      K
	value    V
	forwards []*Node[K, V]
}

func newNode[K cmp.Ordered, V any](key K, value V, level int) *Node[K, V] {
	return &Node[K, V]{
		key:      key,
		value:    value,
		forwards: make([]*Node[K, V], level+1),
	}
}

type SkipList[K cmp.Ordered, V any] struct {
	head  *Node[K, V]
	level int
}

const (
	MaxLevel = 16
	P        = 0.25
)

func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	var nilK K
	var nilV V
	head := newNode(nilK, nilV, MaxLevel)
	return &SkipList[K, V]{head: head, level: 0}
}

func (sl *SkipList[K, V]) randomLevel() int {
	level := 0
	for rand.Float64() < P && level < MaxLevel {
		level++
	}
	return level
}

func (sl *SkipList[K, V]) Insert(key K, value V) {
	update := make([]*Node[K, V], MaxLevel+1)
	current := sl.head

	// 从左上角开始查找
	for i := sl.level; i >= 0; i-- {
		// 从左到右
		for current.forwards[i] != nil && current.forwards[i].key < key {
			current = current.forwards[i]
		}
		update[i] = current
	}

	// current 指向第0层第一个大于 key 的结点
	current = current.forwards[0]
	if current != nil && current.key == key {
		return
	}

	// 为当前要插入的结点生成一个随机层数
	randomLevel := sl.randomLevel()
	if randomLevel > sl.level {
		for i := sl.level + 1; i <= randomLevel; i++ {
			update[i] = sl.head
		}
		sl.level = randomLevel
	}

	newNode := newNode(key, value, randomLevel)

	for i := 0; i <= randomLevel; i++ {
		newNode.forwards[i] = update[i].forwards[i] //新结点指向后面
		update[i].forwards[i] = newNode
	}
}

func (sl *SkipList[K, V]) find(key K) (*Node[K, V], bool) {
	current := sl.head
	for i := sl.level; i >= 0; i-- {
		for current.forwards[i] != nil && current.forwards[i].key < key {
			current = current.forwards[i]
		}
	}
	current = current.forwards[0]
	if current != nil && current.key == key {
		return current, true
	}
	return nil, false
}

func (sl *SkipList[K, V]) Search(key K) (V, bool) {
	current, ok := sl.find(key)
	if !ok {
		var nilV V
		return nilV, false
	}
	return current.value, true
}

func (sl *SkipList[K, V]) Delete(key K) {
	update := make([]*Node[K, V], MaxLevel+1)
	current := sl.head

	for i := sl.level; i >= 0; i-- {
		for current.forwards[i] != nil && current.forwards[i].key < key {
			current = current.forwards[i]
		}
		update[i] = current
	}

	current = current.forwards[0]
	if current == nil || current.key != key {
		return
	}

	for i := 0; i <= sl.level; i++ {
		if update[i].forwards[i] != current {
			break
		}
		update[i].forwards[i] = current.forwards[i]
	}

	for sl.level > 0 && sl.head.forwards[sl.level] == nil {
		sl.level--
	}
}

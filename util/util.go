package util

import (
	"strconv"
)

func DeepCopySlice[T any](in []T) (out []T) {
	out = make([]T, len(in))
	copy(out, in)
	return
}

func GeneratePairs(n int) []struct {
	K []byte
	V []byte
} {
	pairs := make([]struct {
		K []byte
		V []byte
	}, 0, n)
	for i := 0; i < n; i++ {
		pairs = append(pairs, struct {
			K []byte
			V []byte
		}{KeyOf(i), ValueOf(i)})
	}
	return pairs
}

func KeyOf(i int) []byte {
	return []byte("key" + strconv.Itoa(i))
}

func ValueOf(i int) []byte {
	return []byte("value" + strconv.Itoa(i))
}

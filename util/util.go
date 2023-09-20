package util

func DeepCopySlice[T any](in []T) (out []T) {
  out = make([]T, len(in))
  copy(out, in)
  return
}

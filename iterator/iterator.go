package iterator

type Iterator interface {
	Key() []byte
	Value() []byte
	IsValid() bool
	Next()
}

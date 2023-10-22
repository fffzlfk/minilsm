package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Meta struct {
	Offset   uint32
	FirstKey []byte
}

const SizeOfUint32 = 4

func NewBlockMeta(offset uint32, firstKey []byte) *Meta {
	return &Meta{
		Offset:   offset,
		FirstKey: firstKey,
	}
}

func (m *Meta) encode() []byte {
	buf := make([]byte, SizeOfUint32+SizeOfUint16+len(m.FirstKey))
	binary.LittleEndian.PutUint32(buf[:SizeOfUint32], m.Offset)
	binary.LittleEndian.PutUint16(buf[SizeOfUint32:SizeOfUint32+SizeOfUint16], uint16(len(m.FirstKey)))
	copy(buf[SizeOfUint32+SizeOfUint16:], m.FirstKey)
	return buf
}

func EncodeBlockMeta(metas []*Meta) []byte {
	var buf bytes.Buffer
	for _, meta := range metas {
		buf.Write(meta.encode())
	}
	return buf.Bytes()
}

func DecodeBlockMeta(raw []byte) ([]*Meta, error) {
	return DecodeBlockMetaFromReader(bytes.NewReader(raw))
}

func decodeBlock(r io.Reader) (*Meta, error) {
	decodeBlockErrorHandle := func(err error, n int, buf []byte) error {
		if err != nil {
			return fmt.Errorf("decode block: %w", err)
		}
		if n != len(buf) {
			return errors.New("read block data failed")
		}
		return nil
	}

	buf := make([]byte, SizeOfUint32)
	n, err := r.Read(buf)
	if err := decodeBlockErrorHandle(err, n, buf); err != nil {
		return nil, err
	}
	offset := binary.LittleEndian.Uint32(buf)

	buf = make([]byte, SizeOfUint16)
	n, err = r.Read(buf)
	if err := decodeBlockErrorHandle(err, n, buf); err != nil {
		return nil, err
	}
	keySize := binary.LittleEndian.Uint16(buf)

	buf = make([]byte, keySize)
	n, err = r.Read(buf)
	if err := decodeBlockErrorHandle(err, n, buf); err != nil {
		return nil, err
	}

	return &Meta{
		Offset:   offset,
		FirstKey: buf[:keySize],
	}, nil
}

func DecodeBlockMetaFromReader(r io.Reader) ([]*Meta, error) {
	metas := make([]*Meta, 0)
	for {
		meta, err := decodeBlock(r)
		if errors.Is(err, io.EOF) {
			return metas, nil
		}
		if err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}
}

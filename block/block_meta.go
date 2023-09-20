package block

import "encoding/binary"

type Meta struct {
	offset   uint32
	firstKey []byte
}

const SizeOfUint32 = 4

func NewBlockMeta(offset uint32, firstKey []byte) *Meta {
	return &Meta{
		offset:   offset,
		firstKey: firstKey,
	}
}

func (m *Meta) Encode() []byte {
	buf := make([]byte, SizeOfUint32+sizeOfUint16+len(m.firstKey))
	binary.LittleEndian.PutUint32(buf[:SizeOfUint32], m.offset)
	binary.LittleEndian.PutUint16(buf[SizeOfUint32:SizeOfUint32+sizeOfUint16], uint16(len(m.firstKey)))
	copy(buf[SizeOfUint32+sizeOfUint16:], m.firstKey)
	return buf
}

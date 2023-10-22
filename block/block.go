package block

import (
	"encoding/binary"
	"errors"
)

type Block struct {
	data    []byte
	offsets []uint16
}

func (b *Block) bytesSize() uint16 {
	return SizeOfUint16 + uint16(len(b.offsets))*SizeOfUint16 + SizeOfUint16 + uint16(len(b.data))
}

// +--------+--------+--------+-----+--------+-------------+-------+
// | number | offset | offset | ... | offset | data length | data  |
// +--------+--------+--------+-----+--------+-------------+-------+
// | uint16 | uint16 | uint16 | ... | uint16 |   uint16    | bytes |
// +--------+--------+--------+-----+--------+-------------+-------+
func (b *Block) Encode() []byte {
	buf := make([]byte, b.bytesSize())
	idx := uint16(0)
	binary.LittleEndian.PutUint16(buf, uint16(len(b.offsets)))
	idx += SizeOfUint16
	for _, offest := range b.offsets {
		binary.LittleEndian.PutUint16(buf[idx:idx+SizeOfUint16], offest)
		idx += SizeOfUint16
	}
	binary.LittleEndian.PutUint16(buf[idx:idx+SizeOfUint16], uint16(len(b.data)))
	idx += SizeOfUint16
	idx += uint16(copy(buf[idx:], b.data))
	return buf
}

var errDataTooShort = errors.New("binary data is too short")

func (b *Block) Decode(data []byte) error {
	if len(data) < SizeOfUint16 {
		return errDataTooShort
	}
	idx := 0
	offsetsLen := binary.LittleEndian.Uint16(data[idx : idx+SizeOfUint16])
	idx += SizeOfUint16
	if len(data) < idx+int(offsetsLen*SizeOfUint16) {
		return errDataTooShort
	}
	offsets := make([]uint16, offsetsLen)
	for i := 0; i < int(offsetsLen); i++ {
		offsets[i] = binary.LittleEndian.Uint16(data[idx : idx+SizeOfUint16])
		idx += SizeOfUint16
	}
	b.offsets = offsets
	dataLen := binary.LittleEndian.Uint16(data[idx : idx+SizeOfUint16])
	idx += SizeOfUint16
	if len(data) < idx+int(dataLen) {
		return errDataTooShort
	}
	b.data = make([]byte, dataLen)
	copy(b.data, data[idx:idx+int(dataLen)])
	return nil
}

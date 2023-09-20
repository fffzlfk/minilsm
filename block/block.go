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
	return sizeOfUint16 + uint16(len(b.offsets))*sizeOfUint16 + sizeOfUint16 + uint16(len(b.data))
}

func (b *Block) Encode() []byte {
	buf := make([]byte, b.bytesSize())
	idx := uint16(0)
	binary.LittleEndian.PutUint16(buf, uint16(len(b.offsets)))
	idx += sizeOfUint16
	for _, offest := range b.offsets {
		binary.LittleEndian.PutUint16(buf[idx:idx+sizeOfUint16], offest)
		idx += sizeOfUint16
	}
	binary.LittleEndian.PutUint16(buf[idx:idx+sizeOfUint16], uint16(len(b.data)))
	idx += sizeOfUint16
	idx += uint16(copy(buf[idx:], b.data))
	return buf
}

var errDataTooShort = errors.New("binary data is too short")

func (b *Block) Decode(data []byte) error {
	if len(data) < sizeOfUint16 {
		return errDataTooShort
	}
	idx := 0
	offsetsLen := binary.LittleEndian.Uint16(data[idx : idx+sizeOfUint16])
	idx += sizeOfUint16
	if len(data) < idx+int(offsetsLen*sizeOfUint16) {
		return errDataTooShort
	}
	offsets := make([]uint16, offsetsLen)
	for i := 0; i < int(offsetsLen); i++ {
		offsets[i] = binary.LittleEndian.Uint16(data[idx : idx+sizeOfUint16])
		idx += sizeOfUint16
	}
	b.offsets = offsets
	dataLen := binary.LittleEndian.Uint16(data[idx : idx+sizeOfUint16])
	idx += sizeOfUint16
	if len(data) < idx+int(dataLen) {
		return errDataTooShort
	}
	b.data = make([]byte, dataLen)
	copy(b.data, data[idx:idx+int(dataLen)])
	return nil
}

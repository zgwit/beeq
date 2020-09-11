package packet

import (
	"encoding/binary"
	"fmt"
)

func LenLen(rl int) int {
	if rl <= 127 { //0x7F
		return 1
	} else if rl <= 16383 { //0x7F 7F
		return 2
	} else if rl <= 2097151 { //0x7F 7F 7F
		return 3
	} else {
		return 4
	}
}

func ReadRemainLength(b []byte) (int, int, error) {
	length := len(b)
	size := 1
	for {
		if length < size {
			return 0, size, fmt.Errorf("[ReadRemainLength] Expect at leat %d bytes", 1)
		}

		if b[size-1] > 0x80 {
			size += 1
		} else {
			break
		}

		if size > 4 {
			return 0, size, fmt.Errorf("[ReadRemainLength] Expect at most 4 bytes, got %d", size)
		}
	}
	rl, size := binary.Uvarint(b)
	return int(rl), size, nil
}

func WriteRemainLength(b []byte, rl int) (int, error) {
	length := len(b)
	ll := LenLen(rl)
	if ll > length {
		return 0, fmt.Errorf("[ReadRemainLength] Expect at most %d bytes for remain length", ll)
	}
	return binary.PutUvarint(b, uint64(rl)), nil
}

func ReadBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, fmt.Errorf("[readLPBytes] Expect at least %d bytes for prefix", 2)
	}
	length := int(binary.BigEndian.Uint16(buf))
	total := length + 2
	if len(buf) < total {
		return nil, 0, fmt.Errorf("[readLPBytes] Expect at least %d bytes", length+2)
	}
	b := buf[2 : total]
	return b, total, nil
}

func WriteBytes(buf []byte, b []byte) (int, error) {
	length, size := len(b), len(buf)

	if length > 65535 {
		return 0, fmt.Errorf("[writeLPBytes] Too much bytes(%d) to write", length)
	}

	total := length + 2
	if size < total {
		return 0, fmt.Errorf("[writeLPBytes] Expect at least %d bytes", total)
	}

	binary.BigEndian.PutUint16(buf, uint16(length))

	copy(buf[2:], b)

	return total, nil
}

func BytesDup(buf []byte) []byte {
	b := make([]byte, len(buf))
	copy(b, buf)
	return b
}

func Decode(buf []byte) (Message, int, error) {
	mt := MsgType(buf[0] >> 4)
	msg := mt.NewMessage()
	if msg != nil {
		l, err := msg.Decode(buf)
		return msg, l, err
	} else {
		return nil, 0, fmt.Errorf("unknown messege type %d", mt)
	}
}

func Encode(msg Message) ([]byte, []byte, error) {
	return msg.Encode()
}

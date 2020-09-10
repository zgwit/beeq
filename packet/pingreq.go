package packet

import (
	"fmt"
)

type PingReq struct {
	Header
}

func (msg *PingReq) Decode(buf []byte) (int, error) {
	msg.dirty = false

	//Tips. remain length is fixed 0 & total is fixed 2
	total := len(buf)
	if total < 2 {
		return 0, fmt.Errorf("Ping expect fixed 2 bytes, got %d", total)
	}

	offset := 0

	//Header
	msg.header = buf[0]
	offset++

	//Remain Length
	if l, n, err := ReadRemainLength(buf[offset:]); err != nil {
		return offset, err
	} else if l != 0 {
		return 0, fmt.Errorf("Remain length must be 0, got %d", l)
	} else {
		msg.remainLength = l
		offset += n
	}

	// FixHead & VarHead
	msg.head = buf[0:offset]

	return offset, nil
}

func (msg *PingReq) Encode() ([]byte, []byte, error) {
	if !msg.dirty {
		return msg.head, nil, nil
	}

	//Tips. remain length is fixed 0 & total is fixed 2
	//Remain Length
	msg.remainLength = 0

	//FixHead & VarHead
	hl := msg.remainLength

	hl += 1 + LenLen(msg.remainLength)
	//Alloc buffer
	msg.head = make([]byte, hl)

	//Header
	ho := 0
	msg.head[ho] = msg.header
	ho++

	//Remain Length
	if n, err := WriteRemainLength(msg.head[ho:], msg.remainLength); err != nil {
		return nil, nil, err
	} else {
		ho += n
	}

	return msg.head, nil, nil
}

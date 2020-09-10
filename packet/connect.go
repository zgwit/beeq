package packet

import (
	"encoding/binary"
	"fmt"
	"regexp"
)

var clientIdRegex *regexp.Regexp

func init() {
	clientIdRegex = regexp.MustCompile("^[0-9a-zA-Z _]*$")
}

var SupportedVersions map[byte]string = map[byte]string{
	0x3: "MQIsdp",
	0x4: "MQTT",
}

type Connect struct {
	Header

	protoName []byte

	protoLevel byte

	flag byte

	keepAlive uint16

	clientId    []byte
	willTopic   []byte
	willMessage []byte

	userName []byte
	password []byte
}

func (msg *Connect) ProtoName() []byte {
	return msg.protoName
}

func (msg *Connect) SetProtoName(b []byte) {
	msg.dirty = true
	msg.protoName = b
}

func (msg *Connect) ProtoLevel() byte {
	return msg.protoLevel
}

func (msg *Connect) SetProtoLevel(b byte) {
	msg.dirty = true
	msg.protoLevel = b
	msg.protoName = []byte(SupportedVersions[b])
}

func (msg *Connect) UserNameFlag() bool {
	return msg.flag&0x80 == 0x80 //1000 0000
}

func (msg *Connect) SetUserNameFlag(b bool) {
	msg.dirty = true
	if b {
		msg.flag |= 0x80 //1000 0000
	} else {
		msg.flag &= 0x7F //0111 1111
	}
}

func (msg *Connect) PasswordFlag() bool {
	return msg.flag&0x40 == 0x40 //0100 0000
}

func (msg *Connect) SetPasswordFlag(b bool) {
	msg.dirty = true
	if b {
		msg.flag |= 0x40 //0100 0000
	} else {
		msg.flag &= 0xBF //1011 1111
	}
}

func (msg *Connect) WillRetain() bool {
	return msg.flag&0x20 == 0x40 //0010 0000
}

func (msg *Connect) SetWillRetain(b bool) {
	msg.dirty = true
	if b {
		msg.flag |= 0x20 //0010 0000
	} else {
		msg.flag &= 0xDF //1101 1111
	}
	msg.SetWillFlag(true)
}

func (msg *Connect) WillQos() MsgQos {
	return MsgQos((msg.flag & 0x18) >> 2) // 0001 1000
}

func (msg *Connect) SetWillQos(qos MsgQos) {
	msg.dirty = true
	msg.flag &= 0xE7 // 1110 0111
	msg.flag |= byte(qos << 2)
	msg.SetWillFlag(true)
}

func (msg *Connect) WillFlag() bool {
	return msg.flag&0x04 == 0x04 //0000 0100
}

func (msg *Connect) SetWillFlag(b bool) {
	msg.dirty = true
	if b {
		msg.flag |= 0x04 //0000 0100
	} else {
		msg.flag &= 0xFB //1111 1011

		msg.SetWillQos(Qos0)
		msg.SetWillRetain(false)
	}
}

func (msg *Connect) KeepAlive() uint16 {
	return msg.keepAlive
}

func (msg *Connect) SetKeepAlive(k uint16) {
	msg.dirty = true
	msg.keepAlive = k
}

func (msg *Connect) ClientId() []byte {
	return msg.clientId
}

func (msg *Connect) SetClientId(b []byte) {
	msg.dirty = true
	msg.clientId = b
	//msg.ValidClientId()
}

func (msg *Connect) WillTopic() []byte {
	return msg.willTopic
}

func (msg *Connect) SetWillTopic(b []byte) {
	msg.dirty = true
	msg.willTopic = b
	msg.SetWillFlag(true)
}

func (msg *Connect) WillMessage() []byte {
	return msg.willMessage
}

func (msg *Connect) SetWillMessage(b []byte) {
	msg.dirty = true
	msg.willMessage = b
	msg.SetWillFlag(true)
}

func (msg *Connect) UserName() []byte {
	return msg.userName
}

func (msg *Connect) SetUserName(b []byte) {
	msg.dirty = true
	msg.userName = b
	msg.SetUserNameFlag(true)
}

func (msg *Connect) Password() []byte {
	return msg.password
}

func (msg *Connect) SetPassword(b []byte) {
	msg.dirty = true
	msg.password = b
	msg.SetPasswordFlag(true)
}

func (msg *Connect) CleanSession() bool {
	return msg.flag&0x02 == 0x02 //0000 0010
}

func (msg *Connect) SetCleanSession(b bool) {
	msg.dirty = true
	if b {
		msg.flag |= 0x02 //0000 0010
	} else {
		msg.flag &= 0xFD //1111 1101
	}
}

func (msg *Connect) ValidClientId() bool {

	if msg.ProtoLevel() == 0x3 {
		return true
	}

	return clientIdRegex.Match(msg.clientId)
}

func (msg *Connect) Decode(buf []byte) (int, error) {
	msg.dirty = false

	//total := len(buf)
	offset := 0

	//Header
	msg.header = buf[0]
	offset++

	//Remain Length
	if l, n, err := ReadRemainLength(buf[offset:]); err != nil {
		return offset, err
	} else {
		msg.remainLength = l
		offset += n
	}

	//1 Protocol Name
	if b, n, err := ReadBytes(buf[offset:]); err != nil {
		return offset, err
	} else {
		msg.protoName = b
		offset += n
	}

	//2 Protocol Level
	msg.protoLevel = buf[offset]
	if version, ok := SupportedVersions[msg.ProtoLevel()]; !ok {
		return offset, fmt.Errorf("Protocol level (%d) is not support", msg.ProtoLevel())
	} else if ver := string(msg.ProtoName()); ver != version {
		return offset, fmt.Errorf("Protocol name (%s) invalid", ver)
	}
	offset++

	//3 Connect flag
	msg.flag = buf[offset]
	offset++

	if msg.flag&0x1 != 0 {
		return offset, fmt.Errorf("Connect Flags (%x) reserved bit 0", msg.flag)
	}

	if msg.WillQos() > Qos2 {
		return offset, fmt.Errorf("Invalid WillQoS (%d)", msg.WillQos())
	}

	if !msg.WillFlag() && (msg.WillRetain() || msg.WillQos() != Qos0) {
		return offset, fmt.Errorf("Invalid WillFlag (%x)", msg.flag)
	}

	if msg.UserNameFlag() != msg.PasswordFlag() {
		return offset, fmt.Errorf("UserName Password must be both exists or not (%x)", msg.flag)
	}

	//4 Keep Alive
	msg.keepAlive = binary.BigEndian.Uint16(buf[offset:])
	offset += 2

	// FixHead & VarHead
	msg.head = buf[0:offset]
	plo := offset

	//5 ClientId
	if b, n, err := ReadBytes(buf[offset:]); err != nil {
		return offset, err
	} else {
		msg.clientId = b
		offset += n

		// None ClientId, Must Clean Session
		if n == 2 && !msg.CleanSession() {
			return offset, fmt.Errorf("None ClientId, Must Clean Session (%x)", msg.flag)
		}

		// ClientId at most 23 characters
		if n > 128+2 {
			return offset, fmt.Errorf("Too long ClientId (%s)", string(msg.ClientId()))
		}

		// ClientId 0-9, a-z, A-Z
		if n > 0 && !msg.ValidClientId() {
			return offset, fmt.Errorf("Invalid ClientId (%s)", string(msg.ClientId()))
		}
	}

	//6 Will Topic & Message
	if msg.WillFlag() {
		if b, n, err := ReadBytes(buf[offset:]); err != nil {
			return offset, err
		} else {
			msg.willTopic = b
			offset += n
		}

		if b, n, err := ReadBytes(buf[offset:]); err != nil {
			return offset, err
		} else {
			msg.willMessage = b
			offset += n
		}
	}

	//7 UserName & Password
	if msg.UserNameFlag() {
		if b, n, err := ReadBytes(buf[offset:]); err != nil {
			return offset, err
		} else {
			msg.userName = b
			offset += n
		}

		if b, n, err := ReadBytes(buf[offset:]); err != nil {
			return offset, err
		} else {
			msg.password = b
			offset += n
		}
	}

	//Payload
	msg.payload = buf[plo:offset]

	return offset, nil
}

func (msg *Connect) Encode() ([]byte, []byte, error) {
	if !msg.dirty {
		return msg.head, msg.payload, nil
	}

	//Remain Length
	msg.remainLength = 0
	//Protocol Name
	msg.remainLength += 2 + len(msg.protoName)
	//Protocol Level
	msg.remainLength += 1
	//Connect Flags
	msg.remainLength += 1
	//Keep Alive
	msg.remainLength += 1

	//FixHead & VarHead
	hl := msg.remainLength

	//ClientId
	msg.remainLength += 2 + len(msg.clientId)
	//Will Topic & Message
	if msg.WillFlag() {
		msg.remainLength += 2 + len(msg.willTopic)
		msg.remainLength += 2 + len(msg.willMessage)
	}
	//UserName & Password
	if msg.UserNameFlag() {
		msg.remainLength += 2 + len(msg.userName)
		msg.remainLength += 2 + len(msg.password)
	}

	pl := msg.remainLength - hl
	hl += 1 + LenLen(msg.remainLength)

	//Alloc buffer
	msg.head = make([]byte, hl)
	msg.payload = make([]byte, pl)

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

	//1 Protocol Name
	if n, err := WriteBytes(msg.head[ho:], msg.protoName); err != nil {
		return nil, nil, err
	} else {
		ho += n
	}

	//2 Protocol Level
	msg.head[ho] = msg.protoLevel
	ho++

	//3 Connect Flags
	msg.head[ho] = msg.flag
	ho++

	//4 Keep Alive
	binary.BigEndian.PutUint16(msg.head[ho:], msg.keepAlive)
	ho += 2

	plo := 0
	//5 ClientId
	if n, err := WriteBytes(msg.payload[plo:], msg.clientId); err != nil {
		return msg.head, nil, err
	} else {
		plo += n
	}

	//6 Will Topic & Message
	if msg.WillFlag() {
		if n, err := WriteBytes(msg.payload[plo:], msg.willTopic); err != nil {
			return msg.head, nil, err
		} else {
			plo += n
		}

		if n, err := WriteBytes(msg.payload[plo:], msg.willMessage); err != nil {
			return msg.head, nil, err
		} else {
			plo += n
		}
	}

	//7 UserName & Password
	if msg.UserNameFlag() {
		if n, err := WriteBytes(msg.payload[plo:], msg.userName); err != nil {
			return msg.head, nil, err
		} else {
			plo += n
		}

		if n, err := WriteBytes(msg.payload[plo:], msg.password); err != nil {
			return msg.head, nil, err
		} else {
			plo += n
		}
	}

	return msg.head, msg.payload, nil
}

package packet

type MsgType byte

const (
	RESERVED MsgType = iota
	CONNECT
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
	RESERVED2
)

var msgNames = []string{
	"RESERVED", "CONNECT", "CONNACK", "PUBLISH",
	"PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
	"SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK",
	"PINGREQ", "PINGRESP", "DISCONNECT", "RESERVED",
}

func (mt MsgType) Name() string {
	return msgNames[mt&0x0F]
}

func (mt MsgType) NewMessage() Message {
	var msg Message
	switch mt {
	case CONNECT:
		msg = new(Connect)
	case CONNACK:
		msg = new(Connack)
	case PUBLISH:
		msg = new(Publish)
	case PUBACK:
		msg = new(PubAck)
	case PUBREC:
		msg = new(PubRec)
	case PUBREL:
		msg = new(PubRel)
	case PUBCOMP:
		msg = new(PubComp)
	case SUBSCRIBE:
		msg = new(Subscribe)
	case SUBACK:
		msg = new(SubAck)
	case UNSUBSCRIBE:
		msg = new(UnSubscribe)
	case UNSUBACK:
		msg = new(UnSubAck)
	case PINGREQ:
		msg = new(PingReq)
	case PINGRESP:
		msg = new(PingResp)
	case DISCONNECT:
		msg = new(DisConnect)
	default:
		//error
		return nil
	}
	msg.SetType(mt)
	return msg
}

type MsgQos byte

var qosNames = []string{
	"AtMostOnce", "AtLastOnce", "ExactlyOnce", "QosError",
}

func (qos MsgQos) Name() string {
	// 0000 0011
	return qosNames[qos&0x03]
}

func (qos MsgQos) Level() uint8 {
	return uint8(qos & 0x03)
}

const (
	//At most once
	Qos0 MsgQos = iota
	//At least once
	Qos1
	//Exactly once
	Qos2
)

type Message interface {
	Type() MsgType
	SetType(t MsgType)
	Dup() bool
	SetDup(b bool)
	Qos() MsgQos
	SetQos(qos MsgQos)
	Retain() bool
	SetRetain(b bool)
	RemainLength() int

	Decode([]byte) (int, error)

	Encode() ([]byte, []byte, error)
}

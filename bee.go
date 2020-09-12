package beeq

import (
	"encoding/binary"
	"git.zgwit.com/iot/beeq/packet"
	"log"
	"net"
	"sync"
	"time"
)

type Bee struct {
	//Client ID (from CONNECT)
	clientId string

	//Keep Alive (from CONNECT)
	keepAlive int

	//will topic (from CONNECT)
	will *packet.Publish

	//Qos1 Qos2
	pub1 sync.Map // map[uint16]*packet.Publish
	pub2 sync.Map // map[uint16]*packet.Publish

	//Received Qos2 Publish
	recvPub2 sync.Map // map[uint16]*packet.Publish

	//Increment 0~65535
	packetId uint16

	conn net.Conn

	hive *Hive

	timeout time.Duration
}

func NewBee(conn net.Conn, hive *Hive) *Bee {
	return &Bee{
		conn: conn,
		hive: hive,
		//timeout: time.Hour * 24,
	}
}


func (b *Bee) recv(buf []byte) (int, error) {
	err := b.conn.SetReadDeadline(time.Now().Add(b.timeout))
	if err != nil {
		return 0, err
	}
	return b.conn.Read(buf)
}

func (b *Bee) send(buf []byte) (int, error) {
	err := b.conn.SetWriteDeadline(time.Now().Add(b.timeout))
	if err != nil {
		return 0, err
	}
	return b.conn.Write(buf)
}

func (b *Bee) receiver() {
	//Abort error
	//defer func() {
	//	if r := recover(); r != nil {
	//		log.Print("b receiver panic ", r)
	//	}
	//}()


	readHead := true
	buf := Alloc(6)
	offset := 0
	total := 0
	for {
		if l, err := b.conn.Read(buf[offset:]); err != nil {
			log.Print("Receive Failed: ", err)
			break
		} else {
			offset += l
		}

		//Parse header
		if readHead && offset >= 2 {
			for i := 1; i <= offset; i++ {
				if buf[i] < 0x80 {
					rl, rll := binary.Uvarint(buf[1:])
					remainLength := int(rl)
					total = remainLength + rll + 1
					if total > 6 {
						buf = ReAlloc(buf, total)
					}
					readHead = false
					break
				}
			}
		}

		//Parse Message
		if !readHead && offset >= total {
			readHead = true
			bb := Alloc(6)
			if msg, l, err := packet.Decode(buf); err != nil {
				//TODO log err
				log.Println("", err)
				offset = 0 //clear data
			} else {
				//处理消息
				b.handleMessage(msg)

				//Only message less than 6 bytes
				if offset > l {
					copy(bb, bb[l:])
					offset -= l
				} else {
					offset = 0 //clear data
				}
			}
			buf = bb
		}
	}

	b.conn = nil
}

func (b *Bee) handleMessage(msg packet.Message) {
	log.Printf("Received message from %s: %s QOS(%d) DUP(%t) RETAIN(%t)", b.clientId, msg.Type().Name(), msg.Qos(), msg.Dup(), msg.Retain())
	//log.Print("recv Message:", msg.Type().Name())
	switch msg.Type() {
	case packet.CONNECT:
		b.handleConnect(msg.(*packet.Connect))
	case packet.PUBLISH:
		b.handlePublish(msg.(*packet.Publish))
	case packet.PUBACK:
		b.handlePubAck(msg.(*packet.PubAck))
	case packet.PUBREC:
		b.handlePubRec(msg.(*packet.PubRec))
	case packet.PUBREL:
		b.handlePubRel(msg.(*packet.PubRel))
	case packet.PUBCOMP:
		b.handlePubComp(msg.(*packet.PubComp))
	case packet.SUBSCRIBE:
		b.handleSubscribe(msg.(*packet.Subscribe))
	case packet.UNSUBSCRIBE:
		b.handleUnSubscribe(msg.(*packet.UnSubscribe))
	case packet.PINGREQ:
		b.handlePingReq(msg.(*packet.PingReq))
	case packet.DISCONNECT:
		b.handleDisconnect(msg.(*packet.DisConnect))
	}
}

func (b *Bee) handleConnect(msg *packet.Connect) {
	b.clientId = string(msg.ClientId())

	//if msg.WillFlag() {
	//	b.session.will = new(packet.Publish)
	//}
	b.hive.handleConnect(msg, b)
}

func (b *Bee) handlePublish(msg *packet.Publish) {
	qos := msg.Qos()
	if qos == packet.Qos0 {
		b.hive.handlePublish(msg, b)
	} else if qos == packet.Qos1 {
		b.hive.handlePublish(msg, b)
		//Reply PUBACK
		ack := packet.PUBACK.NewMessage().(*packet.PubAck)
		ack.SetPacketId(msg.PacketId())
		b.dispatchMessage(ack)
	} else if qos == packet.Qos2 {
		//Save & Send PUBREC
		b.recvPub2.Store(msg.PacketId(), msg)
		ack := packet.PUBREC.NewMessage().(*packet.PubRec)
		ack.SetPacketId(msg.PacketId())
		b.dispatchMessage(ack)
	} else {
		//error
	}
}

func (b *Bee) handlePubAck(msg *packet.PubAck) {
	if _, ok := b.pub1.Load(msg.PacketId()); ok {
		b.pub1.Delete(msg.PacketId())
	}
}

func (b *Bee) handlePubRec(msg *packet.PubRec) {
	msg.SetType(packet.PUBREL)
	b.dispatchMessage(msg)
}

func (b *Bee) handlePubRel(msg *packet.PubRel) {
	msg.SetType(packet.PUBCOMP)
	b.dispatchMessage(msg)
}

func (b *Bee) handlePubComp(msg *packet.PubComp) {
	if _, ok := b.pub2.Load(msg.PacketId()); ok {
		b.pub2.Delete(msg.PacketId())
	}
}

func (b *Bee) handleSubscribe(msg *packet.Subscribe) {
	b.hive.handleSubscribe(msg, b)
}

func (b *Bee) handleUnSubscribe(msg *packet.UnSubscribe) {
	b.hive.handleUnSubscribe(msg, b)
}

func (b *Bee) handlePingReq(msg *packet.PingReq) {
	msg.SetType(packet.PINGRESP)
	b.dispatchMessage(msg)
}

func (b *Bee) handleDisconnect(msg *packet.DisConnect) {
	b.hive.handleDisconnect(msg, b)
}

func (b *Bee) dispatchMessage(msg packet.Message) {
	log.Printf("Send message to %s: %s QOS(%d) DUP(%t) RETAIN(%t)", b.clientId, msg.Type().Name(), msg.Qos(), msg.Dup(), msg.Retain())
	if head, payload, err := msg.Encode(); err != nil {
		//TODO log
		log.Print("Message encode  error: ", err)
	} else {
		b.send(head)
		if payload != nil && len(payload) > 0 {
			b.send(payload)
		}
	}

	if msg.Type() == packet.PUBLISH {
		pub := msg.(*packet.Publish)
		//Publish Qos1 Qos2 Need store
		if msg.Qos() == packet.Qos1 {
			b.pub1.Store(pub.PacketId(), pub)
		} else if msg.Qos() == packet.Qos2 {
			b.pub2.Store(pub.PacketId(), pub)
		}
	}
}

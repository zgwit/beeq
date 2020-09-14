package beeq

import (
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

	//消息发送队列，避免主协程任务过重
	msgQueue chan packet.Message

	timeout time.Duration
}

func NewBee(conn net.Conn, hive *Hive) *Bee {
	return &Bee{
		conn: conn,
		hive: hive,
		//timeout: time.Hour * 24,
	}
}

//
//func (b *Bee) receiver() {
//	//Abort error
//	//defer func() {
//	//	if r := recover(); r != nil {
//	//		log.Print("b receiver panic ", r)
//	//	}
//	//}()
//
//	readHead := true
//	buf := Alloc(6)
//	offset := 0
//	total := 0
//	for {
//
//		err := b.conn.SetReadDeadline(time.Now().Add(b.timeout))
//		if err != nil {
//			//return 0, err
//			break
//		}
//		if l, err := b.conn.Read(buf[offset:]); err != nil {
//			log.Print("Receive Failed: ", err)
//			//net.ErroTim
//			break
//		} else {
//			offset += l
//		}
//
//		//Parse header
//		if readHead && offset >= 2 {
//			for i := 1; i <= offset; i++ {
//				if buf[i] < 0x80 {
//					rl, rll := binary.Uvarint(buf[1:])
//					//binary.MaxVarintLen32
//					//binary.PutUvarint()
//					remainLength := int(rl)
//					total = remainLength + rll + 1
//					if total > 6 {
//						buf = ReAlloc(buf, total)
//					}
//					readHead = false
//					break
//				}
//			}
//		}
//
//		//Parse Message
//		if !readHead && offset >= total {
//			readHead = true
//			bb := Alloc(6)
//			if msg, l, err := packet.Decode(buf); err != nil {
//				//TODO log err
//				log.Println("", err)
//				offset = 0 //clear data
//			} else {
//				//处理消息
//				b.handleMessage(msg)
//
//				//Only message less than 6 bytes
//				if offset > l {
//					copy(bb, bb[l:])
//					offset -= l
//				} else {
//					offset = 0 //clear data
//				}
//			}
//			buf = bb
//		}
//	}
//
//	b.conn = nil
//}

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
		b.dispatch(ack)
	} else if qos == packet.Qos2 {
		//Save & Send PUBREC
		b.recvPub2.Store(msg.PacketId(), msg)
		ack := packet.PUBREC.NewMessage().(*packet.PubRec)
		ack.SetPacketId(msg.PacketId())
		b.dispatch(ack)
	} else {
		//error
	}
}

func (b *Bee) send(msg packet.Message) {
	//log.Printf("Send message to %s: %s QOS(%d) DUP(%t) RETAIN(%t)", b.clientId, msg.Type().Name(), msg.Qos(), msg.Dup(), msg.Retain())
	if head, payload, err := msg.Encode(); err != nil {
		//TODO log
		log.Print("Message encode  error: ", err)
	} else {
		//err := b.conn.SetWriteDeadline(time.Now().Add(b.timeout))
		_, err = b.conn.Write(head)
		if err != nil {
			//TODO 关闭bee
			//return err
		}
		if payload != nil && len(payload) > 0 {
			_, err = b.conn.Write(payload)
			if err != nil {
				//TODO 关闭bee
				//return err
			}
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

func (b *Bee) dispatch(msg packet.Message) {
	if b.msgQueue != nil {
		b.msgQueue <- msg
		return
	}

	b.send(msg)
}


func (b *Bee) sender()  {
	for b.conn != nil {
		//TODO select or close
		msg := <- b.msgQueue
		b.send(msg)
	}
}

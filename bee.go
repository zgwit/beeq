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

	//消息发送队列，避免主协程任务过重
	msgQueue chan packet.Message

	timeout time.Duration
}

func NewBee(conn net.Conn) *Bee {
	return &Bee{
		conn: conn,
		//timeout: time.Hour * 24,
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

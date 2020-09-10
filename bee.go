package beeq

import (
	"encoding/binary"
	"github.com/zgwit/beeq/packet"
	"log"
	"net"
	"time"
)

type Bee struct {
	clientId string
	session  *Session

	conn net.Conn

	hive *Hive

	quit   chan struct{}
	events chan *Event

	timeout time.Duration

	alive bool
}

func NewBee(conn net.Conn, hive *Hive) *Bee {
	return &Bee{
		conn:    conn,
		hive:    hive,
		quit:    make(chan struct{}),
		events:  make(chan *Event, 1024),
		timeout: time.Hour * 24,
	}
}

func (bee *Bee) Event(event *Event) {
	bee.events <- event
}

func (bee *Bee) Active() {
	bee.alive = true

	go bee.receiver()
	go bee.messenger()
}

func (bee *Bee) Shutdown() {
	bee.alive = false

	// Tell hive delete me
	bee.hive.Event(NewEvent(E_LOST_CONN, nil, bee))

	// Stop messenger
	close(bee.events)
	close(bee.quit)
}

func (bee *Bee) recv(b []byte) (int, error) {
	err := bee.conn.SetReadDeadline(time.Now().Add(bee.timeout))
	if err != nil {
		return 0, err
	}
	return bee.conn.Read(b)
}

func (bee *Bee) send(b []byte) (int, error) {
	err := bee.conn.SetWriteDeadline(time.Now().Add(bee.timeout))
	if err != nil {
		return 0, err
	}
	return bee.conn.Write(b)
}

func (bee *Bee) receiver() {

	//Abort error
	defer func() {
		if r := recover(); r!=nil {
			log.Print("bee receiver panic ", r)
			bee.Shutdown()
		}
	}()

	readHead := true
	buf := Alloc(6)
	offset := 0
	total := 0
	for {
		if l, err := bee.recv(buf[offset:]); err != nil {
			log.Print("Receive Failed: ", err)
			break
		} else {
			offset += l
		}

		//Parse head
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
			b := Alloc(6)
			if msg, l, err := packet.Decode(buf); err != nil {
				//TODO log err
				log.Print(err)
				offset = 0 //clear data
			} else {
				bee.Event(NewEvent(E_MESSAGE, msg, bee))
				//Only message less than 6 bytes
				if offset > l {
					copy(b, buf[l:])
					offset -= l
				} else {
					offset = 0 //clear data
				}
			}
			buf = b
		}
	}

	//Shutdown bee
	if bee.alive {
		bee.Shutdown()
	}
}

func (bee *Bee) messenger() {

	//Abort error
	defer func() {
		if r := recover(); r!=nil {
			log.Print("bee messenger panic ", r)
			bee.Shutdown()
		}
	}()

	for {
		//Blocking
		var event *Event

		select {
		case <-bee.quit:
			bee.conn.Close()
			return
		case event = <-bee.events:
			switch event.event {
			case E_MESSAGE:
				bee.handleMessage(event.data.(packet.Message))
			case E_DISPATCH:
				bee.dispatchMessage(event.data.(packet.Message))
			case E_CLOSE:
				bee.Shutdown()
			}
		}
	}
}

func (bee *Bee) handleMessage(msg packet.Message) {
	log.Printf("Received message from %s: %s QOS(%d) DUP(%t) RETAIN(%t)", bee.clientId, msg.Type().Name(), msg.Qos(), msg.Dup(), msg.Retain())
	//log.Print("recv Message:", msg.Type().Name())
	switch msg.Type() {
	case packet.CONNECT:
		bee.handleConnect(msg.(*packet.Connect))
	case packet.PUBLISH:
		bee.handlePublish(msg.(*packet.Publish))
	case packet.PUBACK:
		bee.handlePubAck(msg.(*packet.PubAck))
	case packet.PUBREC:
		bee.handlePubRec(msg.(*packet.PubRec))
	case packet.PUBREL:
		bee.handlePubRel(msg.(*packet.PubRel))
	case packet.PUBCOMP:
		bee.handlePubComp(msg.(*packet.PubComp))
	case packet.SUBSCRIBE:
		bee.handleSubscribe(msg.(*packet.Subscribe))
	case packet.UNSUBSCRIBE:
		bee.handleUnSubscribe(msg.(*packet.UnSubscribe))
	case packet.PINGREQ:
		bee.handlePingReq(msg.(*packet.PingReq))
	case packet.DISCONNECT:
		bee.handleDisconnect(msg.(*packet.DisConnect))
	}
}

func (bee *Bee) handleConnect(msg *packet.Connect) {
	bee.clientId = string(msg.ClientId())

	//if msg.WillFlag() {
	//	bee.session.will = new(packet.Publish)
	//}
	bee.hive.Event(NewEvent(E_CONNECT, msg, bee))
}

func (bee *Bee) handlePublish(msg *packet.Publish) {
	qos := msg.Qos()
	if qos == packet.Qos0 {
		bee.hive.Event(NewEvent(E_PUBLISH, msg, bee))
	} else if qos == packet.Qos1 {
		bee.hive.Event(NewEvent(E_PUBLISH, msg, bee))
		//Reply PUBACK
		puback := packet.PUBACK.NewMessage().(*packet.PubAck)
		puback.SetPacketId(msg.PacketId())
		bee.Event(NewEvent(E_DISPATCH, puback, bee))
	} else if qos == packet.Qos2 {
		//Save & Send PUBREC
		bee.session.recvPub2[msg.PacketId()] = msg
		pubrec := packet.PUBREC.NewMessage().(*packet.PubRec)
		pubrec.SetPacketId(msg.PacketId())
		bee.Event(NewEvent(E_DISPATCH, pubrec, bee))
	} else {
		//error
	}
}

func (bee *Bee) handlePubAck(msg *packet.PubAck) {
	if _, ok := bee.session.pub1[msg.PacketId()]; ok {
		delete(bee.session.pub1, msg.PacketId())
	}
}

func (bee *Bee) handlePubRec(msg *packet.PubRec) {
	msg.SetType(packet.PUBREL)
	bee.Event(NewEvent(E_DISPATCH, msg, bee))
}

func (bee *Bee) handlePubRel(msg *packet.PubRel) {
	msg.SetType(packet.PUBCOMP)
	bee.Event(NewEvent(E_DISPATCH, msg, bee))
}

func (bee *Bee) handlePubComp(msg *packet.PubComp) {
	if _, ok := bee.session.pub2[msg.PacketId()]; ok {
		delete(bee.session.pub2, msg.PacketId())
	}
}

func (bee *Bee) handleSubscribe(msg *packet.Subscribe) {
	bee.hive.Event(NewEvent(E_SUBSCRIBE, msg, bee))
}

func (bee *Bee) handleUnSubscribe(msg *packet.UnSubscribe) {
	bee.hive.Event(NewEvent(E_UNSUBSCRIBE, msg, bee))
}

func (bee *Bee) handlePingReq(msg *packet.PingReq) {
	msg.SetType(packet.PINGRESP)
	bee.Event(NewEvent(E_DISPATCH, msg, bee))
}

func (bee *Bee) handleDisconnect(msg *packet.DisConnect) {
	bee.hive.Event(NewEvent(E_DISCONNECT, msg, bee))
}

func (bee *Bee) dispatchMessage(msg packet.Message) {
	log.Printf("Send message to %s: %s QOS(%d) DUP(%t) RETAIN(%t)", bee.clientId, msg.Type().Name(), msg.Qos(), msg.Dup(), msg.Retain())
	if head, payload, err := msg.Encode(); err != nil {
		//TODO log
		log.Print("Message encode  error: ", err)
	} else {
		bee.send(head)
		if payload != nil && len(payload) > 0 {
			bee.send(payload)
		}
	}

	if msg.Type() == packet.PUBLISH {
		pub := msg.(*packet.Publish)
		//Publish Qos1 Qos2 Need store
		if msg.Qos() == packet.Qos1 {
			bee.session.pub1[pub.PacketId()] = pub
		} else if msg.Qos() == packet.Qos2 {
			bee.session.pub2[pub.PacketId()] = pub
		}
	}
}

package beeq

import (
	uuid "github.com/satori/go.uuid"
	"github.com/zgwit/beeq/packet"
	"log"
	"net"
	"sync"
	"time"
)

type Hive struct {

	//Subscribe tree
	subTree *SubTree

	//Retain tree
	retainTree *RetainTree

	//ClientId->Bee
	bees sync.Map // map[string]*Bee
}

func NewHive() *Hive {
	return &Hive{
		subTree:    NewSubTree(),
		retainTree: NewRetainTree(),
	}
}

//func (hive *Hive) messenger() {
//	for {
//		select {
//		case <-hive.quit:
//			break
//		case event := <-hive.events:
//			switch event.event {
//			case E_CLOSE:
//				hive.Shutdown()
//			case E_LOST_CONN:
//				hive.handleLostConn(event.from.(*Bee))
//			case E_CONNECT:
//				hive.handleConnect(event.data.(*packet.Connect), event.from.(*Bee))
//			case E_PUBLISH:
//				hive.handlePublish(event.data.(*packet.Publish), event.from.(*Bee))
//			case E_SUBSCRIBE:
//				hive.handleSubscribe(event.data.(*packet.Subscribe), event.from.(*Bee))
//			case E_UNSUBSCRIBE:
//				hive.handleUnSubscribe(event.data.(*packet.UnSubscribe), event.from.(*Bee))
//			case E_DISCONNECT:
//				hive.handleDisconnect(event.data.(*packet.DisConnect), event.from.(*Bee))
//			}
//		}
//	}
//}

func (h *Hive) Serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		//process
		go h.receive(conn)
	}
}

func (h *Hive) receive(conn net.Conn) {



}

func (h *Hive) handleConnect(msg *packet.Connect, bee *Bee) {

	ack := packet.CONNACK.NewMessage().(*packet.Connack)

	//TODO 验证用户名密码

	var clientId string
	if len(msg.ClientId()) == 0 {
		if !msg.CleanSession() {
			//error
			return
		}

		// Generate unique clientId (uuid random)
		for {
			clientId = uuid.NewV4().String()
			if _, ok := h.bees.Load(clientId); !ok {
				break
			}
		}
	} else {
		clientId = string(msg.ClientId())

		if bee, ok := h.bees.Load(clientId); ok {
			// ClientId is already used
			if bee.Alive() {
				//error reject
				ack.SetCode(packet.CONNACK_UNAVAILABLE)

				return
			} else {
				if msg.CleanSession() {
					h.bees.Delete(clientId)
				} else {
					bee.Active(bee)
				}
			}
		}
	}

	log.Println(clientId, " Connected")

	// Generate session
	if _, ok := h.bees[clientId]; !ok {
		session := NewSession()
		h.bees[clientId] = session
	} else {
		ack.SetSessionPresent(true)
	}

	h.bees[clientId].bee = bee
	h.bees[clientId].clientId = clientId
	bee.clientId = clientId
	bee.session = h.bees[clientId]
	if msg.KeepAlive() > 0 {
		bee.timeout = time.Second * time.Duration(msg.KeepAlive()) * 3 / 2
	}

	ack.SetCode(packet.CONNACK_ACCEPTED)
	bee.Send(ack.Encode())

	bee.Event(NewEvent(E_DISPATCH, ack, h))
}

func (h *Hive) handlePublish(msg *packet.Publish, bee *Bee) {
	if err := ValidTopic(msg.Topic()); err != nil {
		//TODO log
		log.Print("Topic invalid ", err)
		return
	}

	if msg.Retain() {
		if len(msg.Payload()) == 0 {
			h.retainTree.UnRetain(bee.clientId)
		} else {
			h.retainTree.Retain(msg.Topic(), bee.clientId, msg)
		}
	}

	//Fetch subscribers
	subs := make(map[string]packet.MsgQos)
	h.subTree.Publish(msg.Topic(), subs)

	//Send publish message
	for clientId, qos := range subs {
		if session, ok := h.bees[clientId]; ok && session.alive {
			bee := session.bee
			//clone new pub
			pub := *msg
			pub.SetRetain(false)
			if msg.Qos() <= qos {
				bee.Event(NewEvent(E_DISPATCH, &pub, h))
			} else {
				pub.SetQos(qos)
				bee.Event(NewEvent(E_DISPATCH, &pub, h))
			}
		}
	}
}

func (h *Hive) handleSubscribe(msg *packet.Subscribe, bee *Bee) {
	suback := packet.SUBACK.NewMessage().(*packet.SubAck)
	suback.SetPacketId(msg.PacketId())
	for _, st := range msg.Topics() {
		log.Print("Subscribe ", string(st.Topic()))
		if err := ValidSubscribe(st.Topic()); err != nil {
			log.Print("Invalid topic ", err)
			//log error
			suback.AddCode(packet.SUB_CODE_ERR)
		} else {
			h.subTree.Subscribe(st.Topic(), bee.clientId, st.Qos())

			suback.AddCode(packet.SubCode(st.Qos()))
			h.retainTree.Fetch(st.Topic(), func(clientId string, pub *packet.Publish) {
				//clone new pub
				p := *pub
				p.SetRetain(true)
				if msg.Qos() <= st.Qos() {
					bee.Event(NewEvent(E_DISPATCH, &p, h))
				} else {
					p.SetQos(st.Qos())
					bee.Event(NewEvent(E_DISPATCH, &p, h))
				}
			})
		}
	}
	bee.Event(NewEvent(E_DISPATCH, suback, h))
}

func (h *Hive) handleUnSubscribe(msg *packet.UnSubscribe, bee *Bee) {
	unsuback := packet.UNSUBACK.NewMessage().(*packet.UnSubAck)
	for _, t := range msg.Topics() {
		log.Print("UnSubscribe ", string(t))
		if err := ValidSubscribe(t); err != nil {
			//TODO log
		} else {
			h.subTree.UnSubscribe(t, bee.clientId)
		}
	}
	bee.Event(NewEvent(E_DISPATCH, unsuback, h))
}

func (h *Hive) handleDisconnect(msg *packet.DisConnect, bee *Bee) {
	bee.Event(NewEvent(E_CLOSE, nil, h))
}

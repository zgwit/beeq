package beeq

import (
	uuid "github.com/google/uuid"
	"git.zgwit.com/iot/beeq/packet"
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

func (h *Hive) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	h.Serve(ln)
	return nil
}

func (h *Hive) Serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		//process
		go h.Receive(conn)
	}
}

func (h *Hive) Receive(conn net.Conn) {
	//TODO 先解析第一个包，而且必须是Connect


	b := NewBee(conn, h)
	go b.receiver()
}

func (h *Hive) handleConnect(msg *packet.Connect, bee *Bee) {

	ack := packet.CONNACK.NewMessage().(*packet.Connack)

	//TODO 验证用户名密码

	var clientId string
	if len(msg.ClientId()) == 0 {

		if !msg.CleanSession() {
			//TODO 无ID，必须是清空会话 error
			//return
		}

		// Generate unique clientId (uuid random)
		clientId = uuid.New().String()
		//UUID不用验重了
		//for { if _, ok := h.bees.Load(clientId); !ok { break } }
	} else {
		clientId = string(msg.ClientId())

		if v, ok := h.bees.Load(clientId); ok {
			b := v.(*Bee)
			// ClientId is already used
			if b.conn != nil {
				//error reject
				ack.SetCode(packet.CONNACK_UNAVAILABLE)
				return
			} else {
				if !msg.CleanSession() {
					//TODO 复制内容
					bee.keepAlive = b.keepAlive
					bee.will = b.will
					bee.pub1 = b.pub1 //sync.Map不能直接复制。。。。
					bee.pub2 = b.pub2
					bee.recvPub2 = b.recvPub2
					bee.packetId = b.packetId
				}
			}
		}

		h.bees.Store(clientId, bee)
	}

	bee.clientId = clientId

	if msg.KeepAlive() > 0 {
		bee.timeout = time.Second * time.Duration(msg.KeepAlive()) * 3 / 2
	}

	//ack.SetCode(packet.CONNACK_ACCEPTED)
	bee.dispatchMessage(ack)

	//TODO 如果发生错误，与客户端断开连接
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
		if b, ok := h.bees.Load(clientId); ok {
			bb := b.(*Bee)
			if bb.conn == nil {
				continue
			}

			//clone new pub
			pub := *msg
			pub.SetRetain(false)
			if msg.Qos() > qos {
				pub.SetQos(qos)
			}
			bb.dispatchMessage(&pub)
		}
	}
}

func (h *Hive) handleSubscribe(msg *packet.Subscribe, bee *Bee) {
	ack := packet.SUBACK.NewMessage().(*packet.SubAck)
	ack.SetPacketId(msg.PacketId())
	for _, st := range msg.Topics() {
		log.Print("Subscribe ", string(st.Topic()))
		if err := ValidSubscribe(st.Topic()); err != nil {
			log.Println("Invalid topic ", err)
			//log error
			ack.AddCode(packet.SUB_CODE_ERR)
		} else {
			h.subTree.Subscribe(st.Topic(), bee.clientId, st.Qos())

			ack.AddCode(packet.SubCode(st.Qos()))
			h.retainTree.Fetch(st.Topic(), func(clientId string, pub *packet.Publish) {
				//clone new pub
				p := *pub
				p.SetRetain(true)
				if msg.Qos() > st.Qos() {
					p.SetQos(st.Qos())
				}
				bee.dispatchMessage(&p)
			})
		}
	}
	bee.dispatchMessage(ack)
}

func (h *Hive) handleUnSubscribe(msg *packet.UnSubscribe, bee *Bee) {
	ack := packet.UNSUBACK.NewMessage().(*packet.UnSubAck)
	for _, t := range msg.Topics() {
		log.Print("UnSubscribe ", string(t))
		if err := ValidSubscribe(t); err != nil {
			//TODO log
		} else {
			h.subTree.UnSubscribe(t, bee.clientId)
		}
	}
	bee.dispatchMessage(ack)
}

func (h *Hive) handleDisconnect(msg *packet.DisConnect, bee *Bee) {
	h.bees.Delete(bee.clientId)
	_ = bee.conn.Close()
}

package beeq

import (
	"encoding/binary"
	uuid "github.com/google/uuid"
	"git.zgwit.com/iot/beeq/packet"
	"log"
	"net"
	"sync"
	"time"
)

func reAlloc(buf []byte, l int) []byte {
	b := make([]byte, l)
	copy(b, buf)
	return b
}

type Hive struct {

	//Subscribe tree
	subTree SubTree

	//Retain tree
	retainTree RetainTree

	//ClientId->Bee
	bees sync.Map // map[string]*Bee

	onConnect     func(*packet.Connect, *Bee) bool
	onPublish     func(*packet.Publish, *Bee) bool
	onSubscribe   func(*packet.Subscribe, *Bee) bool
	onUnSubscribe func(*packet.UnSubscribe, *Bee)
	onDisconnect  func(*packet.DisConnect, *Bee)
}

//TODO 添加参数
func NewHive() *Hive {
	return &Hive{}
}

func (h *Hive) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go h.Serve(ln)
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
	bee := NewBee(conn)

	bufSize := 6
	buf := make([]byte, bufSize)
	of := 0
	for {
		n, err := conn.Read(buf[of:])
		if err != nil {
			log.Println(err)
			break
		}
		ln := of + n

		if ln < 2 {
			of = ln
			continue
		}

		//解析包头，包体

		//读取Remain Length
		rl, rll := binary.Uvarint(buf[1:])
		remainLength := int(rl)
		packLen := remainLength + rll + 1

		//读取未读完的包体
		if packLen > bufSize {
			buf = reAlloc(buf, packLen)

			//直至将全部包体读完
			o := ln
			for o < packLen {
				n, err = conn.Read(buf[o:])
				if err != nil {
					log.Println(err)
					//return
					break
				}
				o += n
			}
			//一般不会发生
			if o < packLen {
				break
			}
		}

		//解析消息
		msg, err := packet.Decode(buf[:packLen])
		if err != nil {
			log.Println(err)
			break
		}

		//处理消息
		//TODO 可以放入队列
		h.handle(msg, bee)

		//解析 剩余内容
		if packLen < bufSize {
			buf = reAlloc(buf[packLen:], bufSize-packLen)
			of = bufSize - packLen
			//TODO 剩余内容可能已经包含了消息，不用再Read，直接解析
		} else {
			buf = make([]byte, bufSize)
		}
	}

	_ = bee.Close()
}

func (h *Hive) handle(msg packet.Message, bee *Bee) {
	switch msg.Type() {
	case packet.CONNECT:
		h.handleConnect(msg.(*packet.Connect), bee)
	case packet.PUBLISH:
		h.handlePublish(msg.(*packet.Publish), bee)
	case packet.PUBACK:
		bee.pub1.Delete(msg.(*packet.PubAck).PacketId())
	case packet.PUBREC:
		msg.SetType(packet.PUBREL)
		bee.dispatch(msg)
	case packet.PUBREL:
		msg.SetType(packet.PUBCOMP)
		bee.dispatch(msg)
	case packet.PUBCOMP:
		bee.pub2.Delete(msg.(*packet.PubComp).PacketId())
	case packet.SUBSCRIBE:
		h.handleSubscribe(msg.(*packet.Subscribe), bee)
	case packet.UNSUBSCRIBE:
		h.handleUnSubscribe(msg.(*packet.UnSubscribe), bee)
	case packet.PINGREQ:
		msg.SetType(packet.PINGRESP)
		bee.dispatch(msg)
	case packet.DISCONNECT:
		h.handleDisconnect(msg.(*packet.DisConnect), bee)
	}
}

func (h *Hive) handleConnect(msg *packet.Connect, bee *Bee) {
	ack := packet.CONNACK.NewMessage().(*packet.Connack)

	//验证用户名密码
	if h.onConnect != nil {
		if !h.onConnect(msg, bee) {
			ack.SetCode(packet.CONNACK_INVALID_USERNAME_PASSWORD)
			bee.dispatch(ack)
			// 断开
			_ = bee.Close()
			return
		}
	}

	var clientId string
	if len(msg.ClientId()) == 0 {

		if !msg.CleanSession() {
			//TODO 无ID，必须是清空会话 error
			//return
			_ = bee.Close()
			return
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
			if !b.closed {
				//error reject
				ack.SetCode(packet.CONNACK_UNAVAILABLE)
				bee.dispatch(ack)
				_ = bee.Close()
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

	//TODO 如果发生错误，与客户端断开连接
	ack.SetCode(packet.CONNACK_ACCEPTED)
	bee.dispatch(ack)
}

func (h *Hive) handlePublish(msg *packet.Publish, bee *Bee) {
	//外部验证
	if h.onPublish != nil {
		if !h.onPublish(msg, bee) {
			return
		}
	}

	qos := msg.Qos()
	if qos == packet.Qos0 {
		//不需要回复puback
	} else if qos == packet.Qos1 {
		//Reply PUBACK
		ack := packet.PUBACK.NewMessage().(*packet.PubAck)
		ack.SetPacketId(msg.PacketId())
		bee.dispatch(ack)
	} else if qos == packet.Qos2 {
		//Save & Send PUBREC
		bee.recvPub2.Store(msg.PacketId(), msg)
		ack := packet.PUBREC.NewMessage().(*packet.PubRec)
		ack.SetPacketId(msg.PacketId())
		bee.dispatch(ack)
	} else {
		//TODO error

	}

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
			bb.dispatch(&pub)
		}
	}
}

func (h *Hive) handleSubscribe(msg *packet.Subscribe, bee *Bee) {
	ack := packet.SUBACK.NewMessage().(*packet.SubAck)
	ack.SetPacketId(msg.PacketId())

	//外部验证
	if h.onSubscribe != nil {
		if !h.onSubscribe(msg, bee) {
			//回复失败
			ack.AddCode(packet.SUB_CODE_ERR)
			bee.dispatch(ack)
			return
		}
	}

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
				bee.dispatch(&p)
			})
		}
	}
	bee.dispatch(ack)
}

func (h *Hive) handleUnSubscribe(msg *packet.UnSubscribe, bee *Bee) {
	//外部验证
	if h.onUnSubscribe != nil {
		h.onUnSubscribe(msg, bee)
	}

	ack := packet.UNSUBACK.NewMessage().(*packet.UnSubAck)
	for _, t := range msg.Topics() {
		log.Print("UnSubscribe ", string(t))
		if err := ValidSubscribe(t); err != nil {
			//TODO log
		} else {
			h.subTree.UnSubscribe(t, bee.clientId)
		}
	}
	bee.dispatch(ack)
}

func (h *Hive) handleDisconnect(msg *packet.DisConnect, bee *Bee) {
	if h.onDisconnect != nil {
		h.onDisconnect(msg, bee)
	}

	h.bees.Delete(bee.clientId)
	_ = bee.Close()
}

func (h *Hive) OnConnect(fn func(*packet.Connect, *Bee) bool) {
	h.onConnect = fn
}
func (h *Hive) OnPublish(fn func(*packet.Publish, *Bee) bool) {
	h.onPublish = fn
}
func (h *Hive) OnSubscribe(fn func(*packet.Subscribe, *Bee) bool) {
	h.onSubscribe = fn
}
func (h *Hive) OnUnSubscribe(fn func(*packet.UnSubscribe, *Bee)) {
	h.onUnSubscribe = fn
}
func (h *Hive) OnDisconnect(fn func(*packet.DisConnect, *Bee)) {
	h.onDisconnect = fn
}

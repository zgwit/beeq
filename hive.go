package beeq

import (

	"log"
	"github.com/zgwit/beeq/packet"
	"time"
)

type Hive struct {
	//Subscribe tree
	subTree *SubTree

	//Retain tree
	retainTree *RetainTree

	//ClientId->Session
	sessions map[string]*Session

	//ClientId->Bee
	//bees map[string]*Bee

	//Message received channel. Waiting for handling
	events chan *Event
	quit   chan struct{}
}

func NewHive() *Hive {
	return &Hive{
		subTree:    NewSubTree(),
		retainTree: NewRetainTree(),
		sessions:   make(map[string]*Session),
		events:     make(chan *Event, 100),
		quit:       make(chan struct{}),
	}
}

func (hive *Hive) messenger() {
	//Abort error
	defer func() {
		if r := recover(); r!=nil {
			log.Print("hive messenger panic ", r)

			//Recovery main routine
			hive.Active()
		}
	}()

	for {
		select {
		case <-hive.quit:
			break
		case event := <-hive.events:
			switch event.event {
			case E_CLOSE:
				hive.Shutdown()
			case E_LOST_CONN:
				hive.handleLostConn(event.from.(*Bee))
			case E_CONNECT:
				hive.handleConnect(event.data.(*packet.Connect), event.from.(*Bee))
			case E_PUBLISH:
				hive.handlePublish(event.data.(*packet.Publish), event.from.(*Bee))
			case E_SUBSCRIBE:
				hive.handleSubscribe(event.data.(*packet.Subscribe), event.from.(*Bee))
			case E_UNSUBSCRIBE:
				hive.handleUnSubscribe(event.data.(*packet.UnSubscribe), event.from.(*Bee))
			case E_DISCONNECT:
				hive.handleDisconnect(event.data.(*packet.DisConnect), event.from.(*Bee))
			}
		}
	}
}

func (hive *Hive) Active() {
	//Single go Routine, no lock
	//Only one processing all message. Performance?
	//TODO Benchmark
	go hive.messenger()
}

func (hive *Hive) Shutdown() {
	close(hive.quit)
}

func (hive *Hive) Event(event *Event) {
	//Blocking
	hive.events <- event
}

func (hive *Hive) handleLostConn(bee *Bee) {
	log.Print("lost ", bee.clientId)
	if session, ok := hive.sessions[bee.clientId]; ok {
		session.DeActive()
	}
}

func (hive *Hive) handleConnect(msg *packet.Connect, bee *Bee) {

	connack := packet.CONNACK.NewMessage().(*packet.Connack)

	var clientId string
	if len(msg.ClientId()) == 0 {
		if !msg.CleanSession() {
			//error
			bee.Event(NewEvent(E_CLOSE, nil, hive))
			return
		}

		// Generate unique clientId (uuid random)
		for {
			clientId = "xxx"
			if _, ok := hive.sessions[clientId]; !ok {
				break
			}
		}

	} else {
		clientId = string(msg.ClientId())

		if session, ok := hive.sessions[clientId]; ok {
			// ClientId is already used
			if session.Alive() {
				//error reject
				connack.SetCode(packet.CONNACK_UNAVAILABLE)
				bee.Event(NewEvent(E_DISPATCH, connack, hive))
				return
			} else {
				if msg.CleanSession() {
					delete(hive.sessions, clientId)
				} else {
					session.Active(bee)
				}
			}
		}
	}

	log.Print(clientId, " Connected")

	// Generate session
	if _, ok := hive.sessions[clientId]; !ok {
		session := NewSession()
		hive.sessions[clientId] = session
	} else {
		connack.SetSessionPresent(true)
	}

	hive.sessions[clientId].bee = bee
	hive.sessions[clientId].clientId = clientId
	bee.clientId = clientId
	bee.session = hive.sessions[clientId]
	if msg.KeepAlive() > 0 {
		bee.timeout = time.Second * time.Duration(msg.KeepAlive()) * 3 / 2
	}

	connack.SetCode(packet.CONNACK_ACCEPTED)
	bee.Event(NewEvent(E_DISPATCH, connack, hive))
}

func (hive *Hive) handlePublish(msg *packet.Publish, bee *Bee) {
	if err := ValidTopic(msg.Topic()); err != nil {
		//TODO log
		log.Print("Topic invalid ", err)
		return
	}

	if msg.Retain() {
		if len(msg.Payload()) == 0 {
			hive.retainTree.UnRetain(bee.clientId)
		} else {
			hive.retainTree.Retain(msg.Topic(), bee.clientId, msg)
		}
	}

	//Fetch subscribers
	subs := make(map[string]packet.MsgQos)
	hive.subTree.Publish(msg.Topic(), subs)

	//Send publish message
	for clientId, qos := range subs {
		if session, ok := hive.sessions[clientId]; ok && session.alive {
			bee := session.bee
			//clone new pub
			pub := *msg
			pub.SetRetain(false)
			if msg.Qos() <= qos {
				bee.Event(NewEvent(E_DISPATCH, &pub, hive))
			} else {
				pub.SetQos(qos)
				bee.Event(NewEvent(E_DISPATCH, &pub, hive))
			}
		}
	}
}

func (hive *Hive) handleSubscribe(msg *packet.Subscribe, bee *Bee) {
	suback := packet.SUBACK.NewMessage().(*packet.SubAck)
	suback.SetPacketId(msg.PacketId())
	for _, st := range msg.Topics() {
		log.Print("Subscribe ", string(st.Topic()))
		if err := ValidSubscribe(st.Topic()); err != nil {
			log.Print("Invalid topic ", err)
			//log error
			suback.AddCode(packet.SUB_CODE_ERR)
		} else {
			hive.subTree.Subscribe(st.Topic(), bee.clientId, st.Qos())

			suback.AddCode(packet.SubCode(st.Qos()))
			hive.retainTree.Fetch(st.Topic(), func(clientId string, pub *packet.Publish) {
				//clone new pub
				p := *pub
				p.SetRetain(true)
				if msg.Qos() <= st.Qos() {
					bee.Event(NewEvent(E_DISPATCH, &p, hive))
				} else {
					p.SetQos(st.Qos())
					bee.Event(NewEvent(E_DISPATCH, &p, hive))
				}
			})
		}
	}
	bee.Event(NewEvent(E_DISPATCH, suback, hive))
}

func (hive *Hive) handleUnSubscribe(msg *packet.UnSubscribe, bee *Bee) {
	unsuback := packet.UNSUBACK.NewMessage().(*packet.UnSubAck)
	for _, t := range msg.Topics() {
		log.Print("UnSubscribe ", string(t))
		if err := ValidSubscribe(t); err != nil {
			//TODO log
		} else {
			hive.subTree.UnSubscribe(t, bee.clientId)
		}
	}
	bee.Event(NewEvent(E_DISPATCH, unsuback, hive))
}

func (hive *Hive) handleDisconnect(msg *packet.DisConnect, bee *Bee) {
	bee.Event(NewEvent(E_CLOSE, nil, hive))
}

package beeq

import (
	"github.com/zgwit/beeq/packet"
	"time"
)

type Session struct {
	//Bee
	bee *Bee

	//Client ID (from CONNECT)
	clientId string

	//Keep Alive (from CONNECT)
	keepAlive int

	//will topic (from CONNECT)
	will *packet.Publish

	//Qos1 Qos2
	pub1 map[uint16]*packet.Publish
	pub2 map[uint16]*packet.Publish

	//Received Qos2 Publish
	recvPub2 map[uint16]*packet.Publish

	//Increment 0~65535
	packetId uint16

	alive bool

	create_time time.Time
	active_time time.Time
	lost_time   time.Time
}

func NewSession() *Session {
	return &Session{
		pub1:        make(map[uint16]*packet.Publish),
		pub2:        make(map[uint16]*packet.Publish),
		recvPub2:    make(map[uint16]*packet.Publish),
		alive:       true,
		create_time: time.Now(),
		active_time: time.Now(),
	}
}

func (session *Session) Alive() bool {
	return session.alive
}

func (session *Session) Active(bee *Bee) {
	session.alive = true
	session.bee = bee
	session.active_time = time.Now()
}

func (session *Session) DeActive() {
	session.alive = false
	session.bee = nil
	session.lost_time = time.Now()
}

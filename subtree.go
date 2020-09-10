package beeq

import (
	"github.com/zgwit/beeq/packet"
	"strings"
)

type SubNode struct {
	//Subscribed clients
	//clientId
	clients map[string]packet.MsgQos

	//Sub level
	//topic->children
	children map[string]*SubNode

	//Multi Wildcard #
	mw *SubNode

	//Single Wildcard +
	sw *SubNode
}

func NewSubNode() *SubNode {
	return &SubNode{
		clients:  make(map[string]packet.MsgQos),
		children: make(map[string]*SubNode),
		//mw: NewSubNode(),
		//sw: NewSubNode(),
	}
}

func (sn *SubNode) Publish(topics []string, subs map[string]packet.MsgQos) {
	if len(topics) == 0 {
		// Publish all matched clients
		for clientId, qos := range sn.clients {
			if sub, ok := subs[clientId]; ok {
				//rewrite by larger Qos
				if sub < qos {
					subs[clientId] = qos
				}
			} else {
				subs[clientId] = qos
			}
		}
	} else {
		name := topics[0]
		// Sub-Level
		if sub, ok := sn.children[name]; ok {
			sub.Publish(topics[1:], subs)
		}
		// Multi wildcard
		if sn.mw != nil {
			sn.mw.Publish(topics[1:1], subs)
		}
		// Single wildcard
		if sn.sw != nil {
			sn.sw.Publish(topics[1:], subs)
		}
	}
}

func (sn *SubNode) Subscribe(topics []string, clientId string, qos packet.MsgQos) {
	if len(topics) == 0 {
		sn.clients[clientId] = qos
		return
	}

	name := topics[0]
	if name == "#" {
		if sn.mw == nil {
			sn.mw = NewSubNode()
		}
		sn.mw.Subscribe(topics[1:1], clientId, qos)
	} else if name == "+" {
		if sn.sw == nil {
			sn.sw = NewSubNode()
		}
		sn.sw.Subscribe(topics[1:], clientId, qos)
	} else {
		if _, ok := sn.children[name]; !ok {
			sn.children[name] = NewSubNode()
		}
		sn.children[name].Subscribe(topics[1:], clientId, qos)
	}
}

func (sn *SubNode) UnSubscribe(topics []string, clientId string) {
	if len(topics) == 0 {
		delete(sn.clients, clientId)
	} else {
		name := topics[0]
		if name == "#" {
			if sn.mw != nil {
				sn.mw.UnSubscribe(topics[1:1], clientId)
			}
		} else if name == "+" {
			if sn.sw != nil {
				sn.sw.UnSubscribe(topics[1:], clientId)
			}
		} else {
			if sub, ok := sn.children[name]; ok {
				sub.UnSubscribe(topics[1:], clientId)
			}
		}
	}
}

func (sn *SubNode) ClearClient(clientId string) {
	if _, ok := sn.clients[clientId]; ok {
		delete(sn.clients, clientId)
	}

	if sn.mw != nil {
		sn.mw.ClearClient(clientId)
	}
	if sn.sw != nil {
		sn.sw.ClearClient(clientId)
	}

	for _, sub := range sn.children {
		sub.ClearClient(clientId)
	}
}

type SubTree struct {
	//tree root
	root *SubNode
}

func NewSubTree() *SubTree {
	return &SubTree{
		root: NewSubNode(),
	}
}

func (st *SubTree) Publish(topic []byte, subs map[string]packet.MsgQos) {
	topics := strings.Split(string(topic), "/")
	if topics[0] == "" {
		topics[0] = "/"
	}
	st.root.Publish(topics, subs)
}

func (st *SubTree) Subscribe(topic []byte, clientId string, qos packet.MsgQos) {
	topics := strings.Split(string(topic), "/")
	if topics[0] == "" {
		topics[0] = "/"
	}
	st.root.Subscribe(topics, clientId, qos)
}

func (st *SubTree) UnSubscribe(topic []byte, clientId string) {
	topics := strings.Split(string(topic), "/")
	if topics[0] == "" {
		topics[0] = "/"
	}
	st.root.UnSubscribe(topics, clientId)
}

func (st *SubTree) ClearClient(clientId string) {
	st.root.ClearClient(clientId)
}

package beeq

import (
	"git.zgwit.com/iot/beeq/packet"
	"strings"
)

type RetainNode struct {

	//Subscribed retains
	//clientId
	retains map[string]*packet.Publish

	//Sub level
	//topic->children
	children map[string]*RetainNode
}

func NewRetainNode() *RetainNode {
	return &RetainNode{
		retains:  make(map[string]*packet.Publish),
		children: make(map[string]*RetainNode),
	}
}

func (rn *RetainNode) Fetch(topics []string, cb func(clientId string, pub *packet.Publish)) {
	if len(topics) == 0 {
		// Publish all matched retains
		for clientId, pub := range rn.retains {
			cb(clientId, pub)
		}
	} else {
		name := topics[0]

		if name == "#" {
			//All retains
			for clientId, pub := range rn.retains {
				cb(clientId, pub)
			}
			//And all children
			for _, sub := range rn.children {
				sub.Fetch(topics, cb)
			}
		} else if name == "+" {
			//Children
			for _, sub := range rn.children {
				sub.Fetch(topics[1:], cb)
			}
		} else {
			// Sub-Level
			if sub, ok := rn.children[name]; ok {
				sub.Fetch(topics[1:], cb)
			}
		}
	}
}

func (rn *RetainNode) Retain(topics []string, clientId string, pub *packet.Publish) *RetainNode {
	if len(topics) == 0 {
		// Publish to specific client
		rn.retains[clientId] = pub
		return rn
	} else {
		name := topics[0]

		// Sub-Level
		if _, ok := rn.children[name]; !ok {
			rn.children[name] = NewRetainNode()
		}
		return rn.children[name].Retain(topics[1:], clientId, pub)
	}
}

type RetainTree struct {
	//root
	root *RetainNode

	//tree index
	//ClientId -> Node (hold Publish message)
	retains map[string]*RetainNode
}

func NewRetainTree() *RetainTree {
	return &RetainTree{
		root: NewRetainNode(),
	}
}

func (rt *RetainTree) Fetch(topic []byte, cb func(clientId string, pub *packet.Publish)) {
	topics := strings.Split(string(topic), "/")
	if topics[0] == "" {
		topics[0] = "/"
	}
	rt.root.Fetch(topics, cb)
}

func (rt *RetainTree) Retain(topic []byte, clientId string, pub *packet.Publish) {
	// Remove last retain publish, firstly
	rt.UnRetain(clientId)

	topics := strings.Split(string(topic), "/")
	if topics[0] == "" {
		topics[0] = "/"
	}
	node := rt.root.Retain(topics, clientId, pub)

	//indexed node
	rt.retains[clientId] = node
}

func (rt *RetainTree) UnRetain(clientId string) {
	if node, ok := rt.retains[clientId]; ok {
		delete(node.retains, clientId)
		delete(rt.retains, clientId)
	}
}

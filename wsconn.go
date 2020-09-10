package beeq

import (
	"golang.org/x/net/websocket"
	"net"
	"time"
)

//inherit net.Conn

type WSConn struct {
	ws  *websocket.Conn
	buf []byte
	quit chan struct{}
}

func NewWSConn(ws *websocket.Conn) *WSConn {
	return &WSConn{
		ws: ws,
		quit: make(chan struct{}),
	}
}

func (conn *WSConn) Read(b []byte) (n int, err error) {
	if conn.buf == nil {
		if err := websocket.Message.Receive(conn.ws, &conn.buf); err != nil {
			return 0, err
		}
	}
	//if rn.buf
	l := copy(b, conn.buf)
	if l < len(conn.buf) {
		conn.buf = conn.buf[l:]
	} else {
		// Receive next packet
		conn.buf = nil
	}
	return l, nil
}

func (conn *WSConn) Write(b []byte) (n int, err error) {
	if err := websocket.Message.Send(conn.ws, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (conn *WSConn) Close() error {
	close(conn.quit)
	return conn.ws.Close()
}

func (conn *WSConn) LocalAddr() net.Addr {
	return conn.ws.LocalAddr()
}

func (conn *WSConn) RemoteAddr() net.Addr {
	return conn.ws.RemoteAddr()
}

func (conn *WSConn) SetDeadline(t time.Time) error {
	return conn.ws.SetDeadline(t)
}

func (conn *WSConn) SetReadDeadline(t time.Time) error {
	return conn.ws.SetReadDeadline(t)
}

func (conn *WSConn) SetWriteDeadline(t time.Time) error {
	return conn.ws.SetWriteDeadline(t)
}

func (conn *WSConn) Wait() {
	<- conn.quit
}


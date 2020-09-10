package beeq

import (
	"crypto/tls"
	"golang.org/x/net/websocket"
	"net"
	"net/http"
	"time"
)


type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}


func AcceptSocket(ln net.Listener, cb func(conn net.Conn)) {
	go (func() {
		//defer ln.Close()
		for {
			if conn, err := ln.Accept(); err != nil {
				//TODO log
				break
			} else {
				cb(conn)
			}
		}
	})()
}

func AcceptWebSocket(ln net.Listener, pattern string, cb func(conn net.Conn)) {
	h := func(ws *websocket.Conn) {
		conn := NewWSConn(ws)
		cb(conn)
		//block here
		conn.Wait()
	}
	mux := http.NewServeMux()
	mux.Handle(pattern, websocket.Handler(h))
	svr := http.Server{Handler:mux}
	go svr.Serve(tcpKeepAliveListener{ln.(*net.TCPListener)})
}

func ListenSocket(laddr string) (net.Listener, error) {
	return net.Listen("tcp", laddr)
}

func ListenSocketTLS(laddr string, cert string, key string) (net.Listener, error) {
	if ce, err := tls.LoadX509KeyPair(cert, key); err != nil {
		return nil, err
	} else {
		config := &tls.Config{Certificates: []tls.Certificate{ce}}
		return tls.Listen("tcp", laddr, config)
	}
}

package gopool

import (
	"sync"
	"time"
)

type IConnFactory interface {
	New(addr string, readTimeout, writeTimeout, connTimeout int64) (IConn, error)
}

type IConn interface {
	Ping() error
	Close() error
}

type Conn struct {
	c          IConn
	mutex      sync.Mutex
	inUse      bool
	accessTime int64
	pool       *Pool
}

func (conn *Conn) GetImpl() interface{} {
	return conn.c
}

func (conn *Conn) expired(keepAlive int64) bool {
	return conn.accessTime+keepAlive < time.Now().Unix()
}

func (conn *Conn) close() error {
	return conn.c.Close()
}

func (conn *Conn) ping() error {
	return conn.c.Ping()
}

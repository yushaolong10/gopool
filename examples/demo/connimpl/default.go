package connimpl

import (
	"encoding/json"
	"fmt"
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
	"time"
)

const (
	KeyBizDemo1 = "demo1biz"
	KeyBizDemo2 = "demo2biz"
)

type GrpcConn struct {
	key      string
	connImpl *ConnImpl
	goConn   *gopool.Conn
}

func (conn *GrpcConn) Ins() *grpc.ClientConn {
	return conn.connImpl.client
}

func GetConn(key string) (*GrpcConn, error) {
	poolCluster, ok := poolClusterList[key]
	if !ok {
		return nil, fmt.Errorf("key(%s) not exist", key)
	}
	conn, err := poolCluster.GetConnBlock()
	if err != nil {
		return nil, err
	}
	connImpl := conn.GetImpl().(*ConnImpl)
	return &GrpcConn{key: key, connImpl: connImpl, goConn: conn}, nil
}

func GetConnTimeout(key string, timeout int64) (*GrpcConn, error) {
	poolCluster, ok := poolClusterList[key]
	if !ok {
		return nil, fmt.Errorf("key(%s) not exist", key)
	}
	conn, err := poolCluster.GetConnTimeout(timeout)
	if err != nil {
		return nil, err
	}
	connImpl := conn.GetImpl().(*ConnImpl)
	return &GrpcConn{key: key, connImpl: connImpl, goConn: conn}, nil
}

func PutConn(conn *GrpcConn, err error) error {
	poolCluster, ok := poolClusterList[conn.key]
	if !ok {
		return fmt.Errorf("key(%s) not exist", conn.key)
	}
	var broken bool
	if err != nil {
		broken = true
	}
	poolCluster.PutConn(conn.goConn, broken)
	return nil
}

var poolClusterList = make(map[string]*gopool.PoolCluster)

func Init() {
	cfg := gopool.Config{
		//for connections
		WriteTimeout: 1000,
		ReadTimeout:  1000,
		ConnTimeout:  1000,
		//for pools
		IdleTimeout: 5000,
		MaxIdleNum:  5,
		MaxConnNum:  10,
	}
	addrList := []string{
		"127.0.0.1:9919",
		"127.0.0.1:9919",
		"127.0.0.1:9918",
		"127.0.0.1:9917",
	}
	poolCluster := gopool.NewPoolCluster(addrList, cfg, &ConnFactory{})
	poolClusterList[KeyBizDemo1] = poolCluster

	go Monitor()
}

func Monitor() {
	for {
		time.Sleep(time.Second * 2)
		for _, pc := range poolClusterList {
			stats := pc.Stats()
			str, _ := json.Marshal(stats)
			fmt.Println("pool cluster stats:", string(str))
		}
	}
}

func Close() {
	for _, poolCluster := range poolClusterList {
		poolCluster.Shutdown()
	}
}

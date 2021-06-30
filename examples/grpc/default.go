package grpc

import (
	"fmt"
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
)

const (
	KeyBizDemo1 = "demo1biz"
	KeyBizDemo2 = "demo2biz"
)

type GrpcConn struct {
	key      string
	grpcImpl *GrpcConnImpl
	goConn   *gopool.Conn
}

func (conn *GrpcConn) Ins() *grpc.ClientConn {
	return conn.grpcImpl.client
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
	grpcConn := conn.GetImpl().(*GrpcConnImpl)
	return &GrpcConn{key: key, grpcImpl: grpcConn, goConn: conn}, nil
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
	grpcConn := conn.GetImpl().(*GrpcConnImpl)
	return &GrpcConn{key: key, grpcImpl: grpcConn, goConn: conn}, nil
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

	//todo monitor

}

func Close() {
	for _, poolCluster := range poolClusterList {
		poolCluster.Shutdown()
	}
}

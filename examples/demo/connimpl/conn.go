package connimpl

import (
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
)

//conn implement
type ConnImpl struct {
	addr        string
	connTimeout int64
	client      *grpc.ClientConn
}

func (g *ConnImpl) Ping() error {
	return nil
}

func (g *ConnImpl) Close() error {
	return nil
}

//factory implement
type ConnFactory struct {
}

func (factory *ConnFactory) New(addr string, readTimeout, writeTimeout, connTimeout int64) (gopool.IConn, error) {
	conn := &grpc.ClientConn{}
	return &ConnImpl{addr: addr, client: conn, connTimeout: connTimeout}, nil
}

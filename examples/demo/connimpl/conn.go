package connimpl

import (
	"fmt"
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
	"math/rand"
	"sync/atomic"
)

var (
	seed int64
	ping int64
)

//conn implement
type ConnImpl struct {
	addr        string
	connTimeout int64
	client      *grpc.ClientConn
}

func (g *ConnImpl) Ping() error {
	source := rand.NewSource(atomic.AddInt64(&seed, 1))
	number := rand.New(source).Intn(5)
	if number == 2 {
		return fmt.Errorf("number 2 ping error")
	}
	ret := atomic.AddInt64(&ping, 1)
	if ret > 20 && g.addr == "127.0.0.1:9917" {
		return fmt.Errorf("127.0.0.1:9917 ping error for removed")
	}
	return nil
}

func (g *ConnImpl) Close() error {
	return nil
}

//factory implement
type ConnFactory struct {
}

func (factory *ConnFactory) New(addr string, readTimeout, writeTimeout, connTimeout int64) (gopool.IConn, error) {
	source := rand.NewSource(atomic.AddInt64(&seed, 1))
	number := rand.New(source).Intn(5)
	if number == 3 {
		return nil, fmt.Errorf("number 3 new error")
	}
	conn := &grpc.ClientConn{}
	return &ConnImpl{addr: addr, client: conn, connTimeout: connTimeout}, nil
}

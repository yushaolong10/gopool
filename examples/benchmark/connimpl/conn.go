package connimpl

import (
	"fmt"
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
	"math/rand"
	"sync/atomic"
	"time"
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
	ret := atomic.AddInt64(&ping, 1)
	if ret > 200 && g.addr == "127.0.0.1:9917" {
		return fmt.Errorf("127.0.0.1:9917 ping error for removed")
	}
	source := rand.NewSource(atomic.AddInt64(&seed, 1))
	number := rand.New(source).Intn(100)
	if number == 2 {
		return fmt.Errorf("number 2 ping error")
	}
	//ping timeUsed about 200us-1.5ms
	source1 := rand.NewSource(atomic.AddInt64(&seed, 1))
	number1 := rand.New(source1).Intn(1000) + 200
	time.Sleep(time.Microsecond * time.Duration(number1))
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
	number := rand.New(source).Intn(100)
	if number == 3 {
		return nil, fmt.Errorf("number 3 new error")
	}
	//removed
	if ping > 300 && addr == "127.0.0.1:9917" {
		return nil, fmt.Errorf("ping>300 127.0.0.1:9917 new removed")
	}
	//new timeUsed about 500us-2ms
	source1 := rand.NewSource(atomic.AddInt64(&seed, 1))
	number1 := rand.New(source1).Intn(1500) + 500
	time.Sleep(time.Microsecond * time.Duration(number1))
	conn := &grpc.ClientConn{}
	return &ConnImpl{addr: addr, client: conn, connTimeout: connTimeout}, nil
}

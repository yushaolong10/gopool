package grpc

import (
	"context"
	"fmt"
	"github.com/alwaysthanks/gopool"
	"google.golang.org/grpc"
	"time"
)

type GrpcConnImpl struct {
	addr        string
	connTimeout int64
	client      *grpc.ClientConn
}

func (g *GrpcConnImpl) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(g.connTimeout)*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, g.addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("ping addr(%s) conn failed,err:%s", g.addr, err.Error())
	}
	if conn != nil {
		conn.Close()
	}
	return nil
}

func (g *GrpcConnImpl) Close() error {
	if g.client != nil {
		g.client.Close()
	}
	return nil
}

type ConnFactory struct {
}

func (factory *ConnFactory) New(addr string, readTimeout, writeTimeout, connTimeout int64) (gopool.IConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connTimeout)*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("create addr(%s) conn failed,err:%s", addr, err.Error())
	}
	return &GrpcConnImpl{addr: addr, client: conn, connTimeout: connTimeout}, nil
}

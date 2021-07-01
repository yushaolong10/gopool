package main

import (
	"github.com/alwaysthanks/gopool/examples/demo/connimpl"
)

func main() {
	//init
	if err := connimpl.Init(); err != nil {

	}
	defer connimpl.Close()

	//usage
	//get biz pool connection
	conn, err := connimpl.GetConn(connimpl.KeyBizDemo1)
	if err != nil {
		//handle error
		return
	}

	//do your business
	//*grpc.ClientConn
	var bizErr error

	conn.Ins()

	//release to the pool
	connimpl.PutConn(conn, bizErr)
}

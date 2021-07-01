# gopool usage example
A golang universal connection pool.

### Usage
> Note: you need to implement the [conn.go](https://github.com/alwaysthanks/gopool/blob/main/conn.go) interface firstly, a specific implementation demo example in package [connimpl](https://github.com/alwaysthanks/gopool/tree/main/examples/demo/connimpl), and see main.go for how to use it

```go
//init pool
if err := connimpl.Init(); err != nil {

}
defer connimpl.Close()

//get connection from pool by biz key
conn, err := connimpl.GetConn(connimpl.KeyBizDemo1)
if err != nil {
    //handle error
}

//do your business

//....
var bizErr error
//*grpc.ClientConn
conn.Ins()
//....

//release connection to the pool
connimpl.PutConn(conn, bizErr)
```
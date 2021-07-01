package main

import (
	"context"
	"fmt"
	"github.com/alwaysthanks/gopool/examples/benchmark/connimpl"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	qps  int64 = 0
	seed int64 = 0
)

func main() {
	connimpl.Init()
	ctx, cancel := context.WithCancel(context.Background())
	go monitor(cancel)
	var wg sync.WaitGroup
	benchmarkBlock(ctx, &wg)
	benchmarkTimeout(ctx, &wg)
	wg.Wait()
	fmt.Println("begin to closed")
	connimpl.Close()
	fmt.Println("closed success. wait 30s to leave...")
	time.Sleep(time.Second * 30)
	fmt.Println("exit.")
}
func monitor(cancel context.CancelFunc) {
	var i int
	for {
		time.Sleep(time.Second)
		i++
		old := atomic.SwapInt64(&qps, 0)
		fmt.Println(i, "qps:", old)
		if i > 600 {
			cancel()
			fmt.Println(i, "canceled")
			return
		}
	}
}

func benchmarkBlock(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go block(ctx, wg)
	}
}

func block(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var err error
		rpc, err := connimpl.GetConn(connimpl.KeyBizDemo1)
		if err != nil {
			fmt.Println("[block] get conn err:", err.Error())
			continue
		}
		source := rand.NewSource(atomic.AddInt64(&seed, 1))
		number := rand.New(source).Intn(20000) + 10000
		time.Sleep(time.Duration(number) * time.Microsecond)
		if number%800 == 0 {
			err = fmt.Errorf("mock error")
		}
		connimpl.PutConn(rpc, err)
		atomic.AddInt64(&qps, 1)
	}
}

func benchmarkTimeout(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go timeout(ctx, wg)
	}
}

func timeout(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		var err error
		rpc, err := connimpl.GetConnTimeout(connimpl.KeyBizDemo1, 30)
		if err != nil {
			fmt.Println("[timeout]timeout get conn err:", err.Error())
			continue
		}
		source := rand.NewSource(atomic.AddInt64(&seed, 1))
		number := rand.New(source).Intn(20000) + 10000
		time.Sleep(time.Duration(number) * time.Microsecond)
		if number%500 == 0 {
			err = fmt.Errorf("mock error")
		}
		connimpl.PutConn(rpc, err)
		atomic.AddInt64(&qps, 1)
	}
}

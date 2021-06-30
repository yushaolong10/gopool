package main

import (
	"fmt"
	"github.com/alwaysthanks/gopool/examples/demo/connimpl"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	connimpl.Init()
	var wg sync.WaitGroup
	doBlock(&wg)
	doTimeout(&wg)
	wg.Wait()
	time.Sleep(10 * time.Second)
	fmt.Println("processor over")
	connimpl.Close()
	fmt.Println("pool closed")
	time.Sleep(10 * time.Second)
}

func doBlock(wg *sync.WaitGroup) {
	var seed int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			rpc, err := connimpl.GetConn(connimpl.KeyBizDemo1)
			if err != nil {
				fmt.Printf("[doBlock]connimpl.GetConn err:%s\n", err.Error())
				return
			}
			source := rand.NewSource(atomic.AddInt64(&seed, 1))
			number := rand.New(source).Intn(20) + 1
			time.Sleep(time.Duration(number) * time.Second)

			//do your business
			fmt.Println("[doBlock] goroutine task business:", j)
			rpc.Ins()

			if number%2 == 0 {
				err = fmt.Errorf("mock error")
			}
			connimpl.PutConn(rpc, err)
		}(i)
	}
}

func doTimeout(wg *sync.WaitGroup) {
	var seed int64 = 2000000
	for i := 200; i < 300; i++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()

			source1 := rand.NewSource(atomic.AddInt64(&seed, 1))
			number1 := rand.New(source1).Intn(10) + 1
			rpc, err := connimpl.GetConnTimeout(connimpl.KeyBizDemo1, int64(number1)*1000)
			if err != nil {
				fmt.Printf("[doTimeout] connimpl.GetConn err:%s\n", err.Error())
				return
			}
			source2 := rand.NewSource(atomic.AddInt64(&seed, 1))
			number2 := rand.New(source2).Intn(20) + 1
			time.Sleep(time.Duration(number2) * time.Second)

			//begin do your business
			fmt.Println("[doTimeout] goroutine task business:", j)
			rpc.Ins()
			// end do your business

			if number2%2 == 0 {
				err = fmt.Errorf("mock error")
			}
			connimpl.PutConn(rpc, err)
		}(i)
	}
}

package gopool

import (
	"context"
	"fmt"
	"github.com/alwaysthanks/melon"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxTryNew = 2
)

var (
	ErrPoolCtxCanceled = fmt.Errorf("pool context canceled")
	ErrPoolClosed      = fmt.Errorf("pool already closed")
	ErrNewConn         = fmt.Errorf("pool new conn error")
	ErrConnTimeout     = fmt.Errorf("pool get conn timeout")
	ErrConnIsNil       = fmt.Errorf("pool conn is nil")
)

type Pool struct {
	ctx    context.Context
	cancel func()
	//meta info
	uniqueId   int64
	addr       string
	keepAlive  int64
	maxIdleNum int64
	maxConnNum int64
	new        func() (IConn, error)

	//total request num and 'return broken' failed num
	requestNum   int64
	reqFailedNum int64
	//call newFunc total num and failed num
	newReqNum    int64
	newFailedNum int64

	//health check server nodes available
	//only use in check, do not need lock
	healthFailedNum int64
	//ping failed num for health check
	pingFailedNum int64
	// if put a new recently active conn into pool when idle list exceed maxIdle,
	// an older conn will be evicted
	evictNum int64
	// currently idle conn numbers
	idleNum int64
	// currently active conn numbers
	activeNum int64
	// currently wait block number when call GetWithBlock that there is not exist idles.
	waitBlockNum int64
	mutex        sync.Mutex
	idleList     []*Conn
	//for block requestId
	reqId     int64
	reqBlocks map[int64]chan *Conn
	//pool closed
	closed bool
	//pool health
	available bool
	//melon metric
	melon *melon.Ring
}

//get connection
//if ctx with timeout, will return ErrConnTimeout when no available conn util the timeout
//if ctx default, will blocked
func (p *Pool) get(ctx context.Context) (conn *Conn, err error) {
	p.mutex.Lock()
	if p.closed {
		p.mutex.Unlock()
		return nil, ErrPoolClosed
	}
	p.requestNum++
	for p.idleNum > 0 {
		//p.idleList
		conn = p.idleList[0]
		copy(p.idleList, p.idleList[1:])
		p.idleList = p.idleList[:p.idleNum-1]
		//idle num
		p.idleNum--
		if conn.expired(p.keepAlive) {
			conn.close()
			//if get conn from idle list expired
			//then just break for new conn
			break
		}
		//active num
		p.activeNum++
		//set inUse true
		conn.inUse = true
		p.mutex.Unlock()
		return conn, nil
	}
	if p.idleNum+p.activeNum >= p.maxConnNum {
		//wait chan
		id := p.nextReqId()
		reqChan := make(chan *Conn, 1)
		//shared reqBlocks map
		p.reqBlocks[id] = reqChan
		p.waitBlockNum++
		p.mutex.Unlock()
		select {
		case <-p.ctx.Done(): //pool canceled
			p.mutex.Lock()
			p.waitBlockNum--
			p.mutex.Unlock()
			return nil, ErrPoolCtxCanceled
		case <-ctx.Done(): //timeout
			p.mutex.Lock()
			p.waitBlockNum--
			_, ok := p.reqBlocks[id]
			if ok {
				delete(p.reqBlocks, id)
			}
			p.mutex.Unlock()
			if !ok {
				select {
				case conn = <-reqChan:
					p.mutex.Lock()
					//it means the chan has been removed with a conn in it,
					//we need put chan conn to other block req
					if len(p.reqBlocks) > 0 {
						var nextChan chan *Conn
						var nextId int64
						for nextId, nextChan = range p.reqBlocks {
							break
						}
						delete(p.reqBlocks, nextId)
						p.mutex.Unlock()

						nextChan <- conn
						log("pool id(%d) addr(%s) transfer conn to other block request", p.uniqueId, p.addr)
					} else {
						if conn != nil {
							//when the conn can not be reused
							//p.activeNum should be minus 1
							p.activeNum--
						}
						p.mutex.Unlock()
						//close later without mutex
						if conn != nil {
							conn.close()
							log("pool id(%d) addr(%s) conn closed because of block request empty", p.uniqueId, p.addr)
						}
					}
				}
			}
			close(reqChan)
			return nil, ErrConnTimeout
		case conn = <-reqChan:
			p.mutex.Lock()
			p.waitBlockNum--
			if conn == nil {
				p.mutex.Unlock()
				return nil, ErrConnIsNil
			}
			p.mutex.Unlock()
			conn.inUse = true
			close(reqChan)
			return conn, nil
		}
	}
	for i := 0; i < maxTryNew; i++ {
		p.newReqNum++
		c, err := p.new()
		if err != nil {
			p.newFailedNum++
			continue
		}
		conn = &Conn{
			c:     c,
			pool:  p,
			inUse: true,
		}
		//active num
		p.activeNum++
		p.mutex.Unlock()
		return conn, nil
	}
	p.mutex.Unlock()
	//set get conn err metric
	p.melon.Feed(false)
	return nil, ErrNewConn
}

//put the connection to pool
//if broken, the connection will be recycled
func (p *Pool) put(conn *Conn, broken bool) {
	//avoid many times put
	conn.mutex.Lock()
	if !conn.inUse {
		conn.mutex.Unlock()
		return
	}

	//add metric
	p.melon.Feed(broken)

	conn.inUse = false
	conn.mutex.Unlock()
	//pool closed
	p.mutex.Lock()
	if p.closed {
		p.mutex.Unlock()
		conn.close()
		return
	}

	//if p.reqBlocks length greater than 0
	//p.activeNum do not need to minus 1
	//because of the p.mutex will unlock early
	//this will lead all conn num exceed maxConnNum
	if len(p.reqBlocks) > 0 {
		var reqChan chan *Conn
		var id int64
		for id, reqChan = range p.reqBlocks {
			break
		}
		delete(p.reqBlocks, id)
		p.mutex.Unlock()

		if broken {
			p.mutex.Lock()
			p.reqFailedNum++
			p.mutex.Unlock()
			//close old
			conn.close()
			conn = nil
			//must get new conn
			//or will make other block get dead lock
			for {
				select {
				case <-p.ctx.Done():
					return
				default:
				}
				//fix: when unavailable, break
				if !p.available {
					//when the conn can not be reused
					//p.activeNum should be minus 1
					p.mutex.Lock()
					p.activeNum--
					p.mutex.Unlock()
					log("pool id(%d) addr(%s) conn broken get new conn but unavailable now", p.uniqueId, p.addr)
					break
				}
				newC, err := p.new()
				p.mutex.Lock()
				p.newReqNum++
				if err != nil {
					p.newFailedNum++
					p.mutex.Unlock()
					log("pool id(%d) addr(%s) conn broken get new conn err:%s", p.uniqueId, p.addr, err.Error())
					continue
				}
				p.mutex.Unlock()
				conn = &Conn{
					c:    newC,
					pool: p,
				}
				break
			}
		}
		reqChan <- conn
		return
	}
	//under some extreme conditions like all conn putted broken
	//then blocked requests maybe deadlock
	if broken {
		p.reqFailedNum++
		p.activeNum--
		p.mutex.Unlock()
		conn.close()
		return
	}
	if p.idleNum >= p.maxIdleNum {
		//p.idleList
		oldConn := p.idleList[0]
		copy(p.idleList, p.idleList[1:])
		p.idleList = append(p.idleList[:p.idleNum-1], conn)
		p.evictNum++
		p.activeNum--
		p.mutex.Unlock()
		//close head older conn
		oldConn.close()
		return
	}
	conn.accessTime = time.Now().Unix()
	p.idleList = append(p.idleList, conn)
	p.idleNum++
	p.activeNum--
	p.mutex.Unlock()
	return
}

//close the pool
func (p *Pool) close() {
	p.mutex.Lock()
	if p.closed {
		p.mutex.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	p.idleNum = 0
	p.activeNum = 0
	p.available = false
	//copy idle list
	idleCopy := make([]*Conn, len(p.idleList))
	copy(idleCopy, p.idleList)
	//idle list nil
	p.idleList = nil
	if len(p.reqBlocks) > 0 {
		for id, reqChan := range p.reqBlocks {
			select {
			case <-reqChan:
			default:
			}
			close(reqChan)
			delete(p.reqBlocks, id)
		}
		p.reqBlocks = nil
	}
	p.mutex.Unlock()
	for _, conn := range idleCopy {
		conn.pool = nil
		conn.close()
	}
	log("pool id(%d) addr(%s) closed success", p.uniqueId, p.addr)
}

func (p *Pool) health() bool {
	func() {
		conn, err := p.new()
		if err != nil {
			log("pool id(%d) addr(%s) health new err:%s", p.uniqueId, p.addr, err.Error())
			p.healthFailedNum++
			return
		}
		err = conn.Ping()
		if err != nil {
			log("pool id(%d) addr(%s) health ping err:%s", p.uniqueId, p.addr, err.Error())
			p.healthFailedNum++
			p.pingFailedNum++
			return
		}
		p.healthFailedNum = 0
	}()
	if p.healthFailedNum > 3 {
		return false
	}
	//add metric check for ok
	if !p.melon.OK() {
		return false
	}
	return true
}

func (p *Pool) isAvailable() bool {
	return p.available
}

func (p *Pool) markAvailable(ok bool) {
	p.available = ok
	//fix: when unavailable, close block req
	if !p.available {
		p.mutex.Lock()
		if len(p.reqBlocks) > 0 {
			for id, reqChan := range p.reqBlocks {
				delete(p.reqBlocks, id)
				close(reqChan)
			}
		}
		p.mutex.Unlock()
	}
}

func (p *Pool) nextReqId() int64 {
	return atomic.AddInt64(&p.reqId, 1)
}

type Stats struct {
	PoolId              int64
	Addr                string
	KeepAlive           int64
	MaxIdleNum          int64
	MaxConnNum          int64
	RequestNum          int64
	ReqFailedNum        int64
	NewReqNum           int64
	NewFailedNum        int64
	PingFailedNum       int64
	HealthFailedNum     int64
	EvictNum            int64
	CurrentIdleNum      int64
	CurrentActiveNum    int64
	CurrentWaitBlockNum int64
	Closed              bool
	Available           bool
}

func (p *Pool) stats() Stats {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return Stats{
		PoolId:              p.uniqueId,
		Addr:                p.addr,
		KeepAlive:           p.keepAlive,
		MaxIdleNum:          p.maxIdleNum,
		MaxConnNum:          p.maxConnNum,
		RequestNum:          p.requestNum,
		ReqFailedNum:        p.reqFailedNum,
		NewReqNum:           p.newReqNum,
		NewFailedNum:        p.newFailedNum,
		PingFailedNum:       p.pingFailedNum,
		HealthFailedNum:     p.healthFailedNum,
		EvictNum:            p.evictNum,
		CurrentIdleNum:      p.idleNum,
		CurrentActiveNum:    p.activeNum,
		CurrentWaitBlockNum: p.waitBlockNum,
		Closed:              p.closed,
		Available:           p.available,
	}
}

func log(format string, a ...interface{}) {
	logFmt := fmt.Sprintf("[gopool] %s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), format)
	fmt.Printf(logFmt, a...)
}

package gopool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClusterEmpty  = fmt.Errorf("poolCluster empty pool")
	ErrPoolClusterClosed = fmt.Errorf("poolCluster closed")
)

type Config struct {
	//for connections
	WriteTimeout int64 //ms
	ReadTimeout  int64 //ms
	ConnTimeout  int64 //ms
	//for pools
	IdleTimeout int64 //ms
	MaxIdleNum  int64
	MaxConnNum  int64
}

func NewPoolCluster(addrList []string, cfg Config, factory IConnFactory) *PoolCluster {
	ctx, cancel := context.WithCancel(context.Background())
	pc := &PoolCluster{
		ctx:      ctx,
		cancel:   cancel,
		poolList: make([]*Pool, 0),
		checkInterval: func() {
			time.Sleep(time.Second * 3)
		},
	}
	for _, addr := range addrList {
		addr := addr
		ctx, cancel := context.WithCancel(ctx)
		p := &Pool{
			uniqueId:   pc.uuid(),
			ctx:        ctx,
			cancel:     cancel,
			addr:       addr,
			keepAlive:  cfg.IdleTimeout / 1000,
			maxIdleNum: cfg.MaxIdleNum,
			maxConnNum: cfg.MaxConnNum,
			new: func() (IConn, error) {
				return factory.New(addr, cfg.ReadTimeout, cfg.WriteTimeout, cfg.ConnTimeout)
			},
			idleList:  make([]*Conn, 0, cfg.MaxIdleNum),
			reqBlocks: make(map[int64]chan *Conn),
			available: true,
		}
		pc.poolList = append(pc.poolList, p)
	}
	go pc.checkAvailable()
	return pc
}

type PoolCluster struct {
	ctx    context.Context
	cancel func()
	//uuid for pool
	nextId int64
	//index for loop
	index int64
	//rw mutex for dynamic update addr
	mutex    sync.RWMutex
	poolList []*Pool
	//health check interval
	checkInterval func()
	//closed
	closed bool
}

//get conn, if exceed maxConnNumbers, will block util conn is ok
func (pc *PoolCluster) GetConnBlock() (conn *Conn, err error) {
	ctx := context.Background()
	p, err := pc.findPool()
	if err != nil {
		return nil, err
	}
	return p.get(ctx)
}

//get conn, if timeout, will return ErrConnTimeout
//timeout (ms)
func (pc *PoolCluster) GetConnTimeout(timeout int64) (conn *Conn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(timeout))
	p, err := pc.findPool()
	if err != nil {
		return nil, err
	}
	return p.get(ctx)
}

//put conn into the pool
func (pc *PoolCluster) PutConn(conn *Conn, broken bool) (err error) {
	p := conn.pool
	if p == nil {
		conn.close()
		log("pool cluster put conn, pool is nil")
	} else {
		p.put(conn, broken)
	}
	return nil
}

//close pool cluster
func (pc *PoolCluster) Shutdown() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	if pc.closed {
		return
	}
	pc.closed = true
	for _, p := range pc.poolList {
		p.close()
	}
	pc.poolList = nil
	//cancel context
	pc.cancel()
	//log
	log("poolCluster closed success")
}

//get a available pool
func (pc *PoolCluster) findPool() (p *Pool, err error) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	if pc.closed {
		return nil, ErrPoolClusterClosed
	}
	poolLen := int64(len(pc.poolList))
	for poolLen > 0 {
		index := atomic.AddInt64(&pc.index, 1)
		p = pc.poolList[index%poolLen]
		if !p.isAvailable() {
			continue
		}
		return p, nil
	}
	return nil, ErrPoolClusterEmpty
}

//health check pool available
func (pc *PoolCluster) checkAvailable() {
	for {
		select {
		case <-pc.ctx.Done():
			return
		default:
		}
		//interval
		pc.checkInterval()
		//check
		pc.mutex.RLock()
		poolLen := len(pc.poolList)
		var i int
		for _, p := range pc.poolList {
			if p.health() {
				p.markAvailable(true)
				continue
			}
			//only 1/3 can mark false
			//not allowed all pool node unavailable
			i++
			if i*3 < 2*poolLen {
				p.markAvailable(false)
			}
		}
		pc.mutex.RUnlock()
	}
}

//get pool cluster stats
func (pc *PoolCluster) Stats() (statsList []Stats) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	statsList = make([]Stats, len(pc.poolList))
	for i, p := range pc.poolList {
		stats := p.stats()
		statsList[i] = stats
	}
	return statsList
}

func (pc *PoolCluster) uuid() (id int64) {
	return atomic.AddInt64(&pc.nextId, 1)
}

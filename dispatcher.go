package rpm

import (
	"bufio"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type dispatcher interface {
	start(*redisModule)
	stop()
	shutdown()
	join()
	dispatch(item interface{})
}

type dispatcherLifecycle struct {
	running int32
	wait    sync.WaitGroup
}

func (dispatcher *dispatcherLifecycle) doStart(start func()) {
	dispatcher.wait.Add(1)
	if !atomic.CompareAndSwapInt32(&dispatcher.running, 0, 1) {
		dispatcher.wait.Done()
	} else {
		go func() {
			defer dispatcher.wait.Done()
			start()
		}()
	}
}

func (dispatcher *dispatcherLifecycle) doStop(stop func()) {
	if !atomic.CompareAndSwapInt32(&dispatcher.running, 1, 0) {
		return
	}
	stop()
}

func (dispatcher *dispatcherLifecycle) join() {
	dispatcher.wait.Wait()
}

type downstreamDispatcher struct {
	dispatcherLifecycle
	requests chan *redisRequest
	pending  chan *redisRequest
	conn     redis.Conn
}

func (dispatcher *downstreamDispatcher) sender() {
	conn := dispatcher.conn
	pending := dispatcher.pending
	requests := dispatcher.requests
	for request := range requests {
		var err error
		for _, do := range request.request {
			if err = conn.Send(do[0].(string), do[1:]...); err != nil {
				request.error(err)
				break
			}
			request.wait++
		}
		if err = conn.Flush(); err != nil {
			/* shouldn't happen */
			panic(err)
		}
		if request.wait > 0 {
			request.buffer = make([]interface{}, request.wait)
			pending <- request
		} else {
			request.done()
		}
	}
}

func (dispatcher *downstreamDispatcher) receiver() {
	conn := dispatcher.conn
	requests := dispatcher.pending
	for pending := range requests {
		for {
			if reply, err := conn.Receive(); err != nil {
				/* shouldn't happen */
				if redisErr, ok := err.(redis.Error); ok {
					pending.error(err)
					if pending.reply(redisErr) {
						break
					}
				} else {
					panic(err)
				}
			} else {
				if pending.reply(reply) {
					break
				}
			}
		}
	}
}

func (dispatcher *downstreamDispatcher) run() {
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		dispatcher.sender()
	}()
	go func() {
		defer wait.Done()
		dispatcher.receiver()
	}()
	wait.Wait()
}

func (dispatcher *downstreamDispatcher) start(module *redisModule) {
	dispatcher.doStart(dispatcher.run)
}

func (dispatcher *downstreamDispatcher) stop() {
	dispatcher.doStop(func() {
		close(dispatcher.requests)
		close(dispatcher.pending)
	})
}

func (dispatcher *downstreamDispatcher) shutdown() {
	dispatcher.stop()
	dispatcher.join()
}

func (dispatcher *downstreamDispatcher) dispatch(item interface{}) {
	dispatcher.requests <- item.(*redisRequest)
}

func newDownstreamDispatcher() dispatcher {
	return &downstreamDispatcher{
		requests: make(chan *redisRequest),
		pending:  make(chan *redisRequest),
		conn:     redis.NewConn(downstream, time.Hour*0xFFFF, time.Hour*0xFFFF),
	}
}

type responseDispatcher struct {
	dispatcherLifecycle
	responses chan []byte
}

func (dispatcher *responseDispatcher) run() {
	for response := range dispatcher.responses {
		upstream.Write(response)
	}
}

func (dispatcher *responseDispatcher) start(module *redisModule) {
	dispatcher.doStart(dispatcher.run)
}

func (dispatcher *responseDispatcher) stop() {
	dispatcher.doStop(func() {
		close(dispatcher.responses)
	})
}

func (dispatcher *responseDispatcher) shutdown() {
	dispatcher.stop()
	dispatcher.join()
}

func (dispatcher *responseDispatcher) dispatch(item interface{}) {
	dispatcher.responses <- item.([]byte)
}

func newResponseDispatcher() dispatcher {
	return &responseDispatcher{
		responses: make(chan []byte),
	}
}

type requestDispatcher struct {
	dispatcherLifecycle
	input *bufio.Reader
}

func (dispatcher *requestDispatcher) run(module *redisModule) {
	var command [][]byte
	var err error
	var clientId, requestId int64
	for {
		if command, err = newRequest(dispatcher.input); err == nil {
			if clientId, err = strconv.ParseInt(string(command[0]), 10, 64); err == nil {
				if requestId, err = strconv.ParseInt(string(command[1]), 10, 64); err == nil {
					module.onRequest(clientId, requestId, command[2:])
				}
				continue
			}
		}
		module.onError(err)
	}
}

func (dispatcher *requestDispatcher) start(module *redisModule) {
	dispatcher.doStart(func() { dispatcher.run(module) })
}

func (dispatcher *requestDispatcher) stop() {
	dispatcher.doStop(func() {
		upstream.Close()
	})
}

func (dispatcher *requestDispatcher) shutdown() {
	dispatcher.stop()
	dispatcher.join()
}

func (dispatcher *requestDispatcher) dispatch(item interface{}) {
}

func newRequestDispatcher() dispatcher {
	return &requestDispatcher{
		input: bufio.NewReader(upstream),
	}
}

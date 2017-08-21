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
	requests chan redisRequest
	conn     redis.Conn
}

func (dispatcher *downstreamDispatcher) run() {
	conn := dispatcher.conn
	for request := range dispatcher.requests {
		reply := make([]interface{}, len(request.request))
		var err error
		for _, request := range request.request {
			if err = conn.Send(request[0].(string), request[1:]...); err != nil {
				goto done
			}
		}
		if err = conn.Flush(); err != nil {
			goto done
		}
		for idx := range reply {
			if reply[idx], err = conn.Receive(); err != nil {
				goto done
			}
		}
	done:
		request.done(reply, err)
	}
}

func (dispatcher *downstreamDispatcher) start(module *redisModule) {
	dispatcher.doStart(dispatcher.run)
}

func (dispatcher *downstreamDispatcher) stop() {
	dispatcher.doStop(func() {
		close(dispatcher.requests)
	})
}

func (dispatcher *downstreamDispatcher) shutdown() {
	dispatcher.stop()
	dispatcher.join()
}

func (dispatcher *downstreamDispatcher) dispatch(item interface{}) {
	dispatcher.requests <- item.(redisRequest)
}

func newDownstreamDispatcher() dispatcher {
	return &downstreamDispatcher{
		requests: make(chan redisRequest),
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

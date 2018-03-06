package rpm

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

type dispatcher interface {
	start(*redisModule)
	stop()
	shutdown()
	join()
	dispatch(item interface{})
}

func forEachDispatcher(dispatchers []dispatcher, f func(dispatcher)) {
	for _, dispatcher := range dispatchers {
		f(dispatcher)
	}
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

func newDownstreamDispatcher(conn net.Conn) dispatcher {
	return &downstreamDispatcher{
		requests: make(chan *redisRequest),
		pending:  make(chan *redisRequest),
		conn:     redis.NewConn(conn, 0, 0),
	}
}

func newDownstreamDispatchers(downstreams []net.Conn) []dispatcher {
	dispatchers := make([]dispatcher, len(downstreams), len(downstreams))
	for idx := range dispatchers {
		dispatchers[idx] = newDownstreamDispatcher(downstreams[idx])
	}
	return dispatchers
}

type responseDispatcher struct {
	dispatcherLifecycle
	responses chan *moduleResponse
	upstream  net.Conn
	worker    RedisModuleWorker
}

func (dispatcher *responseDispatcher) run() {
	upstream := dispatcher.upstream
	profile := make(chan *moduleResponse, 1024)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker := dispatcher.worker
		ctx := context.Background()
		commandProfile := RedisModuleCommandProfile{}
		for response := range profile {
			commandProfile.Command = response.command
			commandProfile.RedisReceiveTime = response.profileTimestamps[0]
			commandProfile.WorkerReceiveTime = response.profileTimestamps[1]
			commandProfile.WorkerProcessTime = response.profileTimestamps[2]
			commandProfile.WorkerSerializeTime = response.profileTimestamps[3]
			commandProfile.WorkerSendTime = response.profileTimestamps[4]
			worker.OnCommandProfile(ctx, &commandProfile)
		}
	}()
	for response := range dispatcher.responses {
		response.write(upstream)
		select {
		case profile <- response:
		default:
			break
		}
	}
	close(profile)
	wg.Wait()
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
	dispatcher.responses <- item.(*moduleResponse)
}

func newResponseDispatcher(worker RedisModuleWorker, upstream net.Conn) dispatcher {
	return &responseDispatcher{
		responses: make(chan *moduleResponse),
		upstream:  upstream,
		worker:    worker,
	}
}

func newResponseDispatchers(worker RedisModuleWorker, upstreams []net.Conn) []dispatcher {
	dispatchers := make([]dispatcher, len(upstreams), len(upstreams))
	for idx := range dispatchers {
		dispatchers[idx] = newResponseDispatcher(worker, upstreams[idx])
	}
	return dispatchers
}

type requestDispatcher struct {
	dispatcherLifecycle
	input    *bufio.Reader
	upstream net.Conn
}

func (dispatcher *requestDispatcher) run(module *redisModule) {
	var command [][]byte
	var err error
	var clientId, requestId, timestamp int64
	for {
		if command, err = newRequest(dispatcher.input); err == nil {
			if clientId, err = strconv.ParseInt(string(command[0]), 10, 64); err == nil {
				if requestId, err = strconv.ParseInt(string(command[1]), 10, 64); err == nil {
					if timestamp, err = strconv.ParseInt(string(command[2]), 10, 64); err == nil {
						module.onRequest(clientId, requestId, timestamp, command[3:])
					}
				}
				continue
			}
		} else {
			panic(fmt.Sprintf("Suicide because it can not be recovered from error: %v", err))
		}
		module.onError(err)
	}
}

func (dispatcher *requestDispatcher) start(module *redisModule) {
	dispatcher.doStart(func() { dispatcher.run(module) })
}

func (dispatcher *requestDispatcher) stop() {
	dispatcher.doStop(func() {
		dispatcher.upstream.Close()
	})
}

func (dispatcher *requestDispatcher) shutdown() {
	dispatcher.stop()
	dispatcher.join()
}

func (dispatcher *requestDispatcher) dispatch(item interface{}) {
}

func newRequestDispatcher(upstream net.Conn) dispatcher {
	return &requestDispatcher{
		input:    bufio.NewReader(upstream),
		upstream: upstream,
	}
}

func newRequestDispatchers(upstreams []net.Conn) []dispatcher {
	dispatchers := make([]dispatcher, len(upstreams), len(upstreams))
	for idx := range dispatchers {
		dispatchers[idx] = newRequestDispatcher(upstreams[idx])
	}
	return dispatchers
}

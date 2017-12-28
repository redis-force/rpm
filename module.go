package rpm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var moduleMutex sync.Mutex
var moduleInstance *redisModule = nil

type contextKey int

var sessionIdKey contextKey = 0

const slowCommandThreshold = time.Second * 10

type redisModule struct {
	requests       []dispatcher
	responses      []dispatcher
	downstreams    []dispatcher
	worker         RedisModuleWorker
	logger         *log.Logger
	downstreamId   uint32
	numDownstreams uint32
}

func (module *redisModule) downstream() dispatcher {
	return module.downstreams[atomic.AddUint32(&module.downstreamId, 1)%module.numDownstreams]
}

func (module *redisModule) commandCleanup(clientId, requestId int64, cmd []byte, startTime time.Time) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("Failed to dispatch request %v(%v) from client %v:%v\n%s", cmd, requestId, clientId, r, string(debug.Stack()))
		module.logger.Print(msg)
		response := newResponse(clientId, requestId)
		response.WriteError("ERR Internal")
		module.responses[clientId%int64(len(module.responses))].dispatch(response.serialize())
	}
	elapsed := time.Now().Sub(startTime)
	if elapsed > slowCommandThreshold {
		module.logger.Printf("Slow command: %v(%v) from client %v took %v\n", cmd, requestId, clientId, elapsed)
	}
}

func (module *redisModule) onRequest(clientId, requestId int64, request [][]byte) {
	startTime := time.Now()
	response := newResponse(clientId, requestId)
	go func() {
		defer module.commandCleanup(clientId, requestId, request[0], startTime)
		ctx := context.WithValue(context.Background(), sessionIdKey, clientId)
		module.worker.OnCommand(ctx, module.NewRedisClient(), request, response)
		module.responses[clientId%int64(len(module.responses))].dispatch(response.serialize())
	}()
}

func (module *redisModule) onError(err error) {
	module.worker.OnError(context.Background(), err)
}

func (module *redisModule) start(d dispatcher) {
	d.start(module)
}

func (module *redisModule) Start() {
	forEachDispatcher(module.downstreams, module.start)
	forEachDispatcher(module.responses, module.start)
	forEachDispatcher(module.requests, module.start)
	module.worker.OnStart(context.Background(), module.NewRedisClient())
}

func (module *redisModule) stop(d dispatcher) {
	d.stop()
}

func (module *redisModule) Stop() {
	module.worker.OnStop(context.Background(), module.NewRedisClient())
	forEachDispatcher(module.requests, module.stop)
	forEachDispatcher(module.responses, module.stop)
	forEachDispatcher(module.downstreams, module.stop)
}

func (module *redisModule) shutdown(d dispatcher) {
	d.shutdown()
}

func (module *redisModule) Shutdown() {
	forEachDispatcher(module.downstreams, module.shutdown)
	forEachDispatcher(module.responses, module.shutdown)
	forEachDispatcher(module.requests, module.shutdown)
}

func (module *redisModule) join(d dispatcher) {
	d.join()
}

func (module *redisModule) Join() {
	forEachDispatcher(module.downstreams, module.join)
	forEachDispatcher(module.responses, module.join)
	forEachDispatcher(module.requests, module.join)
}

func (module *redisModule) NewRedisClient() RedisClient {
	return &redisClient{
		module: module,
		future: make(chan redisResponse),
	}
}

func connectToMany(size, template string) ([]net.Conn, error) {
	if sz, ok := os.LookupEnv(size); !ok {
		return nil, errors.New(size + " is not set")
	} else if sz, err := strconv.Atoi(sz); err != nil {
		return nil, errors.New("Invalid " + size + " = " + strconv.Itoa(sz))
	} else {
		connections := make([]net.Conn, sz, sz)
		for idx := range connections {
			if connections[idx], err = connectTo(fmt.Sprintf(template, idx)); err != nil {
				return nil, err
			}
		}
		return connections, nil
	}
}

func connectTo(env string) (net.Conn, error) {
	var conn net.Conn
	if fd, ok := os.LookupEnv(env); !ok {
		return nil, errors.New(env + " is not set")
	} else if fd, err := strconv.Atoi(fd); err != nil {
		return nil, errors.New("Invalid " + env + " = " + strconv.Itoa(fd))
	} else if conn, err = net.FileConn(os.NewFile((uintptr)(fd), "redis-downstream")); err != nil {
		return nil, err
	}
	return conn, nil
}

func newModule(worker RedisModuleWorker, logger *log.Logger) (RedisModule, error) {
	moduleMutex.Lock()
	defer moduleMutex.Unlock()
	if moduleInstance != nil {
		return moduleInstance, nil
	}
	var err error
	var upstreams []net.Conn
	var downstreams []net.Conn
	if downstreams, err = connectToMany("RPM_DOWNSTREAM_FD_NUM", "RPM_DOWNSTREAM_FD[%d]"); err != nil {
		return nil, err
	}
	if upstreams, err = connectToMany("RPM_UPSTREAM_FD_NUM", "RPM_UPSTREAM_FD[%d]"); err != nil {
		return nil, err
	}

	return &redisModule{
		worker:         worker,
		logger:         logger,
		downstreams:    newDownstreamDispatchers(downstreams),
		responses:      newResponseDispatchers(upstreams),
		requests:       newRequestDispatchers(upstreams),
		numDownstreams: uint32(len(downstreams)),
	}, nil
}

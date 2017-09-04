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
)

var moduleMutex sync.Mutex
var moduleInstance *redisModule = nil

type contextKey int

var sessionIdKey contextKey = 0

type redisModule struct {
	request        dispatcher
	response       dispatcher
	downstreams    []dispatcher
	worker         RedisModuleWorker
	logger         *log.Logger
	downstreamId   uint32
	numDownstreams uint32
}

func (module *redisModule) downstream() dispatcher {
	return module.downstreams[atomic.AddUint32(&module.downstreamId, 1)%module.numDownstreams]
}

func (module *redisModule) panicHandler(id int64) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("Failed to dispatch request %v:%v\n%s", id, r, string(debug.Stack()))
		module.logger.Print(msg)
		response := newResponse(id)
		response.WriteError("ERR Internal")
		module.response.dispatch(response.serialize())
	}
}

func (module *redisModule) onRequest(client int64, id int64, request [][]byte) {
	response := newResponse(id)
	go func() {
		defer module.panicHandler(id)
		ctx := context.WithValue(context.Background(), sessionIdKey, client)
		module.worker.OnCommand(ctx, module.NewRedisClient(), request, response)
		module.response.dispatch(response.serialize())
	}()
}

func (module *redisModule) onError(err error) {
	module.worker.OnError(context.Background(), err)
}

func (module *redisModule) Start() {
	for _, downstream := range module.downstreams {
		downstream.start(module)
	}
	module.response.start(module)
	module.request.start(module)
	module.worker.OnStart(context.Background(), module.NewRedisClient())
}

func (module *redisModule) Stop() {
	module.worker.OnStop(context.Background(), module.NewRedisClient())
	module.request.stop()
	module.response.stop()
	for _, downstream := range module.downstreams {
		downstream.stop()
	}
}

func (module *redisModule) Shutdown() {
	for _, downstream := range module.downstreams {
		downstream.shutdown()
	}
	module.response.shutdown()
	module.request.shutdown()
}

func (module *redisModule) Join() {
	for _, downstream := range module.downstreams {
		downstream.join()
	}
	module.response.join()
	module.request.join()
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
	var upstream net.Conn
	var downstreams []net.Conn
	if downstreams, err = connectToMany("RPM_DOWNSTREAM_FD_NUM", "RPM_DOWNSTREAM_FD[%d]"); err != nil {
		return nil, err
	}
	if upstream, err = connectTo("RPM_UPSTREAM_FD"); err != nil {
		return nil, err
	}

	return &redisModule{
		worker:         worker,
		logger:         logger,
		downstreams:    newDownstreamDispatchers(downstreams),
		response:       newResponseDispatcher(upstream),
		request:        newRequestDispatcher(upstream),
		numDownstreams: uint32(len(downstreams)),
	}, nil
}

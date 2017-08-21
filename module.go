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
)

var moduleMutex sync.Mutex
var moduleInstance *redisModule = nil
var downstream net.Conn = nil
var upstream net.Conn = nil

type contextKey int

var sessionIdKey contextKey = 0

type redisModule struct {
	request    dispatcher
	response   dispatcher
	downstream dispatcher
	worker     RedisModuleWorker
	logger     *log.Logger
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
	redis := &redisClient{
		module: module,
		future: make(chan redisResponse),
	}
	go func() {
		defer module.panicHandler(id)
		ctx := context.WithValue(context.Background(), sessionIdKey, client)
		module.worker.OnCommand(ctx, redis, request, response)
		module.response.dispatch(response.serialize())
	}()
}

func (module *redisModule) onError(err error) {
	module.worker.OnError(context.Background(), err)
}

func (module *redisModule) Start() {
	module.downstream.start(module)
	module.response.start(module)
	module.request.start(module)
	redis := &redisClient{
		module: module,
		future: make(chan redisResponse),
	}
	module.worker.OnStart(context.Background(), redis)
}

func (module *redisModule) Stop() {
	redis := &redisClient{
		module: module,
		future: make(chan redisResponse),
	}
	module.worker.OnStop(context.Background(), redis)
	module.request.stop()
	module.response.stop()
	module.downstream.stop()
}

func (module *redisModule) Shutdown() {
	module.downstream.shutdown()
	module.response.shutdown()
	module.request.shutdown()
}

func (module *redisModule) Join() {
	module.downstream.join()
	module.response.join()
	module.request.join()
}

func connectTo(env string) (net.Conn, error) {
	var client net.Conn
	if fd, ok := os.LookupEnv(env); !ok {
		return nil, errors.New(env + " is not set")
	} else if fd, err := strconv.Atoi(fd); err != nil {
		return nil, errors.New("Invalid " + env + " = " + strconv.Itoa(fd))
	} else if client, err = net.FileConn(os.NewFile((uintptr)(fd), "redis")); err != nil {
		return nil, err
	}
	return client, nil
}

func newModule(worker RedisModuleWorker, logger *log.Logger) (RedisModule, error) {
	moduleMutex.Lock()
	defer moduleMutex.Unlock()
	if moduleInstance != nil {
		return moduleInstance, nil
	}
	var err error
	if downstream, err = connectTo("RPM_DOWNSTREAM_FD"); err != nil {
		return nil, err
	}
	if upstream, err = connectTo("RPM_UPSTREAM_FD"); err != nil {
		return nil, err
	}

	return &redisModule{
		worker:     worker,
		logger:     logger,
		downstream: newDownstreamDispatcher(),
		response:   newResponseDispatcher(),
		request:    newRequestDispatcher(),
	}, nil
}

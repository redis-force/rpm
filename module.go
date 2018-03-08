package rpm

import (
	"bufio"
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

func (module *redisModule) commandCleanup(clientId, requestId int64, cmd []byte, commandTimestamp int64, startTime time.Time) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("Failed to dispatch request '%s'@%d from redis client %d:%v\n%s", string(cmd), requestId, clientId, r, string(debug.Stack()))
		module.logger.Print(msg)
		response := newRPMResponse(clientId, requestId, cmd, commandTimestamp, startTime.UnixNano()/1000)
		response.WriteError("ERR Internal")
		module.responses[clientId%int64(len(module.responses))].dispatch(response.serialize())
	}
	elapsed := time.Now().Sub(startTime)
	if elapsed > slowCommandThreshold {
		module.logger.Printf("Slow command: '%s'@%d from redis client %d took %v\n", string(cmd), requestId, clientId, elapsed)
	}
}

func (module *redisModule) onRequest(clientId, requestId, timestamp int64, request [][]byte) {
	startTime := time.Now()
	response := newRPMResponse(clientId, requestId, request[0], timestamp, startTime.UnixNano()/1000)
	go func() {
		response.updateProcessTime()
		defer module.commandCleanup(clientId, requestId, request[0], timestamp, startTime)
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

func (module *redisModule) serve(socket net.Conn, clientId int64) {
	reader := bufio.NewReader(socket)
	writer := bufio.NewWriter(socket)
	var lastCommand []byte
	startTimeUs := new(int64)
	ctx := context.WithValue(context.Background(), sessionIdKey, clientId)
	redisClient := module.NewRedisClient()
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("Failed to dispatch request '%s' from client %v:%v\n%s", string(lastCommand), socket.RemoteAddr(), r, string(debug.Stack()))
			module.logger.Print(msg)
			response := newDirectResponse(*startTimeUs, *startTimeUs)
			response.WriteError("ERR Internal")
			response.serialize()
			writer.Write(response.serialized)
			writer.Flush()
			module.onError(errors.New(msg))
		}
		socket.Close()
	}()
	commandProfile := RedisModuleCommandProfile{}
	for {
		if request, err := newRequest(reader); err != nil {
			module.onError(err)
			return
		} else {
			startTime := time.Now()
			*startTimeUs = startTime.UnixNano() / 1000
			lastCommand = request[0]
			response := newDirectResponse(*startTimeUs, *startTimeUs)
			response.updateProcessTime()
			module.worker.OnCommand(ctx, redisClient, request, response)
			response.serialize()
			if _, err = writer.Write(response.serialized); err != nil {
				module.onError(err)
				return
			}
			if err = writer.Flush(); err != nil {
				module.onError(err)
				return
			}
			commandProfile.Command = request[0]
			commandProfile.RedisReceiveTime = *startTimeUs
			commandProfile.WorkerReceiveTime = *startTimeUs
			commandProfile.WorkerProcessTime = *startTimeUs
			commandProfile.WorkerSerializeTime = response.profileTimestamps[3]
			commandProfile.WorkerSendTime = time.Now().UnixNano() / 1000
			module.worker.OnCommandProfile(ctx, &commandProfile)
		}
	}
}

func (module *redisModule) ListenAndServe(addr string) error {
	if l, err := net.Listen("tcp", addr); err != nil {
		return err
	} else {
		defer l.Close()
		var clientId int64
		for {
			if c, err := l.Accept(); err != nil {
				if e, ok := err.(net.Error); ok && e.Temporary() {
					module.logger.Printf("Failed to accept a new connection: %v\n", e)
					continue
				} else {
					return err
				}
			} else {
				go module.serve(c, clientId)
				clientId++
			}
		}
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
		responses:      newResponseDispatchers(worker, upstreams),
		requests:       newRequestDispatchers(upstreams),
		numDownstreams: uint32(len(downstreams)),
	}, nil
}

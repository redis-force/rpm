package rpm

import (
	"context"
	"io/ioutil"
	"log"
)

type RedisModule interface {
	Start()
	Stop()
	Join()
	Shutdown()
	NewRedisClient() RedisClient
	ListenAndServe(address string) error
}

type RedisClient interface {
	Request(command string, args ...interface{}) []interface{}
	Do(ctx context.Context, command string, args ...interface{}) (reply interface{}, err error)
	DoMulti(ctx context.Context, requests ...[]interface{}) (reply []interface{}, err error)
	Int(reply interface{}, err error) (int, error)
	Int64(reply interface{}, err error) (int64, error)
	Uint(reply interface{}, err error) (uint, error)
	Uint64(reply interface{}, err error) (uint64, error)
	Float64(reply interface{}, err error) (float64, error)
	String(reply interface{}, err error) (string, error)
	ByteSlice(reply interface{}, err error) ([]byte, error)
	Bool(reply interface{}, err error) (bool, error)
	Values(reply interface{}, err error) ([]interface{}, error)
	Strings(reply interface{}, err error) ([]string, error)
	ByteSlices(reply interface{}, err error) ([][]byte, error)
	Ints(reply interface{}, err error) ([]int, error)
	StringMap(result interface{}, err error) (map[string]string, error)
	IntMap(result interface{}, err error) (map[string]int, error)
	Int64Map(result interface{}, err error) (map[string]int64, error)
	Positions(result interface{}, err error) ([]*[2]float64, error)
}

type RedisModuleResponse interface {
	WriteString(s string) error
	WriteBytes(p []byte) error
	WriteError(msg string) error
	WriteInt64(n int64) error
	WriteFloat64(n float64) error
	WriteArrayLength(length int) error
	WriteArray(array []interface{}) error
	WriteBool(val bool) error
	WriteNil() error
	Write(any interface{}) error
}

func SessionIdFromContext(ctx context.Context) int64 {
	return ctx.Value(sessionIdKey).(int64)
}

type RedisModuleCommandProfile struct {
	Command             []byte
	RedisReceiveTime    int64
	WorkerReceiveTime   int64
	WorkerProcessTime   int64
	WorkerSerializeTime int64
	WorkerSendTime      int64
}

type RedisModuleWorker interface {
	OnStart(ctx context.Context, redis RedisClient)
	OnCommand(ctx context.Context, redis RedisClient, args [][]byte, response RedisModuleResponse)
	OnError(ctx context.Context, err error)
	OnStop(ctx context.Context, redis RedisClient)
	OnCommandProfile(ctx context.Context, profile *RedisModuleCommandProfile)
}

func NewModuleWithLogger(worker RedisModuleWorker, logger *log.Logger) (module RedisModule, err error) {
	if logger == nil {
		logger = log.New(ioutil.Discard, "", 0)
	}
	return newModule(worker, logger)
}

func NewModule(worker RedisModuleWorker) (module RedisModule, err error) {
	return NewModuleWithLogger(worker, nil)
}

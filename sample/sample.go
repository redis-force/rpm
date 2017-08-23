package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/redis-force/rpm"
)

type worker struct {
	logger *log.Logger
	index  int
}

var nest = 0

func generator0(response rpm.RedisModuleResponse) {
	response.WriteString("hellostring")
}

func generator1(response rpm.RedisModuleResponse) {
	response.WriteBytes([]byte("hellobytes"))
}

func generator2(response rpm.RedisModuleResponse) {
	if nest == 1 {
		response.WriteError("helloerror")
	} else {
		response.WriteString("helloerror")
	}
}

func generator3(response rpm.RedisModuleResponse) {
	response.WriteInt64(100000000)
}

func generator4(response rpm.RedisModuleResponse) {
	response.WriteFloat64(0.11456)
}

func generator5(response rpm.RedisModuleResponse) {
	resp := make([]interface{}, 4, 4)
	resp[0] = "a"
	resp[1] = "bc"
	resp[2] = 1
	resp[3] = 4
	response.WriteArray(resp)
}

func generator6(response rpm.RedisModuleResponse) {
	response.WriteBool(true)
}

func generator7(response rpm.RedisModuleResponse) {
	response.WriteNil()
}

func generator8(response rpm.RedisModuleResponse) {
	if nest > 6 {
		response.WriteString(fmt.Sprintf("terminate at nest level %d", nest))
		return
	}
	response.WriteArrayLength(20)
	for i := 0; i < 20; i++ {
		generate(i%10, response)
	}
}

func generator9(response rpm.RedisModuleResponse) {
	response.WriteString("helloworld 2")
}

func generate(index int, response rpm.RedisModuleResponse) {
	nest++
	switch index % 10 {
	case 0:
		generator0(response)
	case 1:
		generator1(response)
	case 2:
		generator2(response)
	case 3:
		generator3(response)
	case 4:
		generator4(response)
	case 5:
		generator5(response)
	case 6:
		generator6(response)
	case 7:
		generator7(response)
	case 8:
		generator8(response)
	case 9:
		generator9(response)
	}
	nest--
}

func (w *worker) OnCommand(ctx context.Context, redis rpm.RedisClient, args [][]byte, response rpm.RedisModuleResponse) {
	w.logger.Printf("received request and generate response with type of %v", w.index)
	generate(w.index, response)
	w.index++
	loops := w.index % 100
	w.logger.Printf("send %d info commands to upstream", loops)
	requests := make([][]interface{}, loops)
	for i := 0; i < loops; i++ {
		requests[i] = redis.Request("info")
	}
	if reply, err := redis.DoMulti(ctx, requests...); err != nil {
		panic(err)
	} else {
		for idx, r := range reply {
			w.logger.Printf("Reply[%d]:\n%v", idx, string(r.([]byte)))
		}
	}
}

func (w *worker) OnError(ctx context.Context, err error) {
	w.logger.Printf("caught error %v\n", err)
}

func (w *worker) OnStart(ctx context.Context, redis rpm.RedisClient) {
}

func (w *worker) OnStop(ctx context.Context, redis rpm.RedisClient) {
}

func main() {
	logger := log.New(os.Stdout, "", log.Lshortfile)
	if module, err := rpm.NewModuleWithLogger(&worker{logger: logger, index: 0}, logger); err != nil {
		logger.Print(err)
		return
	} else {
		module.Start()
		module.Join()
	}
}

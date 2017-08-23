package rpm

import (
	"context"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
)

type redisRequest struct {
	request  [][]interface{}
	response chan redisResponse
	buffer   []interface{}
	index    int
	wait     int
	err      error
}

type redisResponse struct {
	reply []interface{}
	err   error
}

func (request *redisRequest) error(err error) {
	request.err = err
}

func (request *redisRequest) done() {
	request.response <- redisResponse{reply: request.buffer, err: request.err}
}

func (request *redisRequest) reply(reply interface{}) bool {
	request.buffer[request.index] = reply
	request.index++
	if request.index == request.wait {
		request.done()
		return true
	} else {
		return false
	}
}

type redisClient struct {
	module *redisModule
	future chan redisResponse
}

var errNil = errors.New("returned nil")

type Error string

func (err Error) Error() string { return string(err) }

func (client *redisClient) Request(command string, args ...interface{}) []interface{} {
	request := make([]interface{}, len(args)+1)
	request[0] = command
	copy(request[1:], args)
	return request
}

func (client *redisClient) Do(ctx context.Context, command string, args ...interface{}) (reply interface{}, err error) {
	if response, err := client.DoMulti(ctx, client.Request(command, args...)); err != nil {
		return nil, err
	} else {
		return response[0], err
	}
}

func (client *redisClient) DoMulti(ctx context.Context, requests ...[]interface{}) (reply []interface{}, err error) {
	client.module.downstream.dispatch(&redisRequest{request: requests, response: client.future})
	responses := <-client.future
	return responses.reply, responses.err
}

func (client *redisClient) Int(reply interface{}, err error) (int, error) {
	return redis.Int(reply, err)
}

func (client *redisClient) Int64(reply interface{}, err error) (int64, error) {
	return redis.Int64(reply, err)
}

func (client *redisClient) Uint(reply interface{}, err error) (uint, error) {
	if err != nil {
		return 0, err
	}
	switch reply := reply.(type) {
	case uint64:
		x := uint(reply)
		if uint64(x) != reply {
			return 0, strconv.ErrRange
		}
		return x, nil
	case []byte:
		n, err := strconv.ParseUint(string(reply), 10, 0)
		return uint(n), err
	case nil:
		return 0, errNil
	case Error:
		return 0, reply
	}
	return 0, fmt.Errorf("redigo: unexpected type for Int, got type %T", reply)
}

func (client *redisClient) Uint64(reply interface{}, err error) (uint64, error) {
	return redis.Uint64(reply, err)
}

func (client *redisClient) Float64(reply interface{}, err error) (float64, error) {
	return redis.Float64(reply, err)
}

func (client *redisClient) String(reply interface{}, err error) (string, error) {
	return redis.String(reply, err)
}

func (client *redisClient) ByteSlice(reply interface{}, err error) ([]byte, error) {
	return redis.Bytes(reply, err)
}

func (client *redisClient) Bool(reply interface{}, err error) (bool, error) {
	return redis.Bool(reply, err)
}

func (client *redisClient) Values(reply interface{}, err error) ([]interface{}, error) {
	return redis.Values(reply, err)
}

func (client *redisClient) Strings(reply interface{}, err error) ([]string, error) {
	return redis.Strings(reply, err)
}

func (client *redisClient) ByteSlices(reply interface{}, err error) ([][]byte, error) {
	return redis.ByteSlices(reply, err)
}

func (client *redisClient) Ints(reply interface{}, err error) ([]int, error) {
	return redis.Ints(reply, err)
}

func (client *redisClient) StringMap(reply interface{}, err error) (map[string]string, error) {
	return redis.StringMap(reply, err)
}

func (client *redisClient) IntMap(reply interface{}, err error) (map[string]int, error) {
	return redis.IntMap(reply, err)
}

func (client *redisClient) Int64Map(reply interface{}, err error) (map[string]int64, error) {
	return redis.Int64Map(reply, err)
}

func (client *redisClient) Positions(reply interface{}, err error) ([]*[2]float64, error) {
	return redis.Positions(reply, err)
}

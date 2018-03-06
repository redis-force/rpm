package rpm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

var nativeEndian binary.ByteOrder

const INT_SIZE int = int(unsafe.Sizeof(0))

func init() {
	var i int = 0x1
	bs := (*[INT_SIZE]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		nativeEndian = binary.BigEndian
	} else {
		nativeEndian = binary.LittleEndian
	}
}

type protocolError string

func (err protocolError) Error() string {
	return fmt.Sprintf("rpm: %s", string(err))
}

func readLine(input *bufio.Reader) ([]byte, error) {
	slice, err := input.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			err = protocolError("long response line")
		}
		return nil, err
	}
	i := len(slice) - 2
	if i < 0 {
		return nil, protocolError(fmt.Sprintf("bad response line terminator. length of line %d is shorter than 2", i+2))
	}
	if slice[i] != '\r' {
		return nil, protocolError(fmt.Sprintf("bad response line terminator. The second last character '%x' is not \\r", slice[i]))
	}
	return slice[:i], nil
}

func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func readBulkString(input *bufio.Reader) ([]byte, error) {
	line, err := readLine(input)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	if line[0] != '$' {
		return nil, protocolError(fmt.Sprintf("command not in correct format, got type '%x'", line[0]))
	}
	length, err := parseLen(line[1:])
	if length < 0 || err != nil {
		return nil, err
	}
	slice := make([]byte, length)
	_, err = io.ReadFull(input, slice)
	if line, err = readLine(input); err != nil {
		return nil, err
	} else if len(line) != 0 {
		return nil, protocolError("bad bulk string format")
	}
	return slice, err
}

func newRequest(input *bufio.Reader) ([][]byte, error) {
	line, err := readLine(input)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short request line")
	}
	if line[0] != '*' {
		return nil, protocolError(fmt.Sprintf("command not in correct format, got type '%x'", line[0]))
	}
	size, err := parseLen(line[1:])
	if size < 0 || err != nil {
		return nil, err
	}

	command := make([][]byte, size)
	for idx := range command {
		command[idx], err = readBulkString(input)
		if err != nil {
			return nil, err
		}
	}
	return command, nil
}

func newQuotaStack() []int {
	quotaStack := make([]int, 1)
	quotaStack[0] = 1
	return quotaStack
}

func newQuota(stack []int, quota int) []int {
	stack = append(stack, quota)
	return stack
}

func consumeQuota(stack []int) []int {
	l := len(stack) - 1
	stack[l]--
	if stack[l] == 0 {
		return stack[:l]
	} else {
		return stack
	}
}

func consumeAllQuota(stack []int) []int {
	stack = consumeQuota(stack)
	for idx, _ := range stack {
		stack[idx] = 0
	}
	return stack
}

type moduleResponse struct {
	command                 []byte
	response                *bytes.Buffer
	lenBuffer               [32]byte
	numBuffer               [40]byte
	quotaStack              []int
	profileTimestamps       [5]int64
	profileTimestampsOffset int
	serialized              []byte
}

func writeInt64ToBuffer(prefix byte, n int64, buffer []byte) int {
	idx := len(buffer) - 1
	buffer[idx] = '\n'
	buffer[idx-1] = '\r'
	idx = idx - 2
	for {
		buffer[idx] = byte('0' + n%10)
		idx -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	buffer[idx] = prefix
	return len(buffer) - idx
}

func newResponse(clientId, requestId int64, command []byte, commandTimestamp, workerStartTimestamp int64) *moduleResponse {
	var header [130]byte
	idx := len(header)
	header[idx-1] = '\n'
	header[idx-2] = '\r'
	header[idx-35] = '\n'
	header[idx-36] = '\r'
	header[idx-37] = '2'
	header[idx-38] = '3'
	header[idx-39] = '$'
	idx -= 39
	idx -= writeInt64ToBuffer(':', requestId, header[:idx])
	idx -= writeInt64ToBuffer(':', clientId, header[:idx])
	header[idx-1] = '\n'
	header[idx-2] = '\r'
	header[idx-3] = '4'
	header[idx-4] = '*'
	slice := header[idx-4:]
	return &moduleResponse{
		command:                 command,
		response:                bytes.NewBuffer(slice),
		quotaStack:              newQuotaStack(),
		profileTimestamps:       [5]int64{commandTimestamp, workerStartTimestamp, 0, 0, 0},
		profileTimestampsOffset: len(slice) - 34,
	}
}

func (response *moduleResponse) updateProfileTimeAt(index int) {
	response.profileTimestamps[index] = time.Now().UnixNano() / 1000
}

func (response *moduleResponse) updateProcessTime() {
	response.updateProfileTimeAt(2)
}

func (response *moduleResponse) newQuota(quota int) {
	response.quotaStack = newQuota(response.quotaStack, quota)
}

func (response *moduleResponse) consumeQuota() {
	response.quotaStack = consumeQuota(response.quotaStack)
}

func (response *moduleResponse) consumeAllQuota() {
	response.quotaStack = consumeAllQuota(response.quotaStack)
}

func (response *moduleResponse) serialize() *moduleResponse {
	for _, quota := range response.quotaStack {
		if quota != 0 {
			panic(fmt.Sprintf("Invalid Response: %v\n", response.quotaStack))
		}
	}
	response.updateProfileTimeAt(3)
	response.serialized = response.response.Bytes()
	return response
}

func (response *moduleResponse) write(upstream io.Writer) {
	profileData := response.serialized[response.profileTimestampsOffset : response.profileTimestampsOffset+32]
	response.updateProfileTimeAt(4)
	for i, ts := range response.profileTimestamps {
		if i == 0 {
			nativeEndian.PutUint64(profileData, uint64(ts))
		} else {
			var offset = ts - response.profileTimestamps[0]
			if offset < 0 {
				offset = 0
			}
			nativeEndian.PutUint32(profileData[4*(i+1):], uint32(offset))
		}
	}
	nativeEndian.PutUint32(profileData[24:], uint32(len(response.serialized)))
	upstream.Write(response.serialized)
}

func (response *moduleResponse) writeLen(prefix byte, n int) error {
	idx := len(response.lenBuffer) - writeInt64ToBuffer(prefix, int64(n), response.lenBuffer[:])
	_, err := response.response.Write(response.lenBuffer[idx:])
	return err
}

func (response *moduleResponse) WriteString(s string) error {
	response.consumeQuota()
	response.writeLen('$', len(s))
	response.response.WriteString(s)
	_, err := response.response.WriteString("\r\n")
	return err
}

func (response *moduleResponse) WriteBytes(p []byte) error {
	response.consumeQuota()
	response.writeLen('$', len(p))
	response.response.Write(p)
	_, err := response.response.WriteString("\r\n")
	return err
}

func (response *moduleResponse) WriteError(msg string) error {
	response.consumeQuota()
	response.response.WriteByte('-')
	_, err := response.response.WriteString(msg)
	if err != nil {
		return err
	}
	_, err = response.response.WriteString("\r\n")
	return err
}

func (response *moduleResponse) WriteInt64(n int64) error {
	return response.WriteBytes(strconv.AppendInt(response.numBuffer[:0], n, 10))
}

func (response *moduleResponse) WriteFloat64(n float64) error {
	return response.WriteBytes(strconv.AppendFloat(response.numBuffer[:0], n, 'g', -1, 64))
}

func (response *moduleResponse) WriteArrayLength(length int) error {
	response.consumeQuota()
	response.newQuota(length)
	return response.writeLen('*', length)
}

func (response *moduleResponse) WriteArray(array []interface{}) error {
	length := len(array)
	response.WriteArrayLength(length)
	for _, elem := range array {
		if err := response.Write(elem); err != nil {
			return err
		}
	}
	return nil
}

func (response *moduleResponse) WriteBool(val bool) error {
	if val {
		return response.WriteString("1")
	} else {
		return response.WriteString("0")
	}
}

func (response *moduleResponse) WriteNil() error {
	return response.WriteString("")
}

func (response *moduleResponse) Write(any interface{}) (err error) {
	switch elem := any.(type) {
	case string:
		return response.WriteString(elem)
	case []byte:
		return response.WriteBytes(elem)
	case int:
		return response.WriteInt64(int64(elem))
	case int64:
		return response.WriteInt64(elem)
	case float64:
		return response.WriteFloat64(elem)
	case bool:
		return response.WriteBool(elem)
	case nil:
		return response.WriteString("")
	default:
		tp := reflect.TypeOf(any)
		switch tp.Kind() {
		case reflect.Slice:
			fallthrough
		case reflect.Array:
			return response.WriteArray(any.([]interface{}))
		}
		var buf bytes.Buffer
		fmt.Fprint(&buf, elem)
		return response.WriteBytes(buf.Bytes())
	}
}

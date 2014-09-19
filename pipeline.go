package redis

import (
    "fmt"
    "io"
    "net"
    "bufio"
    "strconv"
)

func (client *Client) Pipeline() *Pipeline {
    p := &Pipeline{
        client: client,
        stack : make([]operand, 0),
    }
    return p
}

type operand struct {
    command string
    args []string
}

type Pipeline struct {
    client *Client
    stack []operand
}

func (p *Pipeline) executeCommand(command string, args...string) {
    op := operand{command, args}
    p.stack = append(p.stack, op)
}

func (p *Pipeline) rawSend(c net.Conn, cmds []byte) ([]interface{}, error) {
    _, err := c.Write(cmds)
    if err != nil {
        return nil, err
    }
    reader := bufio.NewReaderSize(c, 4096*1024)
    responses := make([]interface{}, len(p.stack))
    for i := 0; i < len(p.stack); i++ {
        responses[i], err = readResponse(reader)
        if err != nil {
            responses[i] = err
        }
    }
    return responses, nil
}

func (p *Pipeline) sendCommands(cmds []byte) (data []interface{}, err error) {
    c, err := p.client.popCon()
	defer p.client.pushConAndClean(c,err)
    if err != nil {
        println(err.Error())
        goto End
    }

    data, err = p.rawSend(c, cmds)
    if err == io.EOF {
        c, err = p.client.openConnection()
        if err != nil {
            println(err.Error())
            goto End
        }

        data, err = p.rawSend(c, cmds)
    }

End:
    return
}

func (p *Pipeline) Reset() {
    p.stack = make([]operand, 0)
}

func (p *Pipeline) Execute() ([]interface{}, error) {
    if len(p.stack) == 0 {
        return nil, nil
    }
    rawBytes := []byte{}
    for _, op := range p.stack {
        rawBytes = append(rawBytes, commandBytes(op.command, op.args...)...)
    }
    data, err := p.sendCommands(rawBytes)
    p.Reset()
    return data, err
}

func (p *Pipeline) Exists(key string) *Pipeline {
    p.executeCommand("EXISTS", key)
    return p
}

func (p *Pipeline) Del(key string) *Pipeline {
    p.executeCommand("DEL", key)
    return p
}

func (p *Pipeline) Type(key string) *Pipeline {
    p.executeCommand("TYPE", key)
    return p
}

func (p *Pipeline) Keys(key string) *Pipeline {
    p.executeCommand("KEYS", key)
    return p
}

func (p *Pipeline) Expire(key string, timeout int64) *Pipeline {
    p.executeCommand("EXPIRE", key, strconv.FormatInt(timeout, 10))
    return p
}

func (p *Pipeline) Set(key string, value []byte) *Pipeline {
    p.executeCommand("SET", key, string(value))
    return p
}

func (p *Pipeline) Get(key string) *Pipeline {
    p.executeCommand("GET", key)
    return p
}

func (p *Pipeline) Incr(key string) *Pipeline {
    p.executeCommand("INCR", key)
    return p
}

func (p *Pipeline) Incrby(key string, value int64) *Pipeline {
    p.executeCommand("INCRBY", key, strconv.FormatInt(value, 10))
    return p
}

func (p *Pipeline) Decr(key string) *Pipeline {
    p.executeCommand("DECR", key)
    return p
}

func (p *Pipeline) Decrby(key string, value int64) *Pipeline {
    p.executeCommand("DECRBY", key, strconv.FormatInt(value, 10))
    return p
}

func (p *Pipeline) Append(key string, value []byte) *Pipeline {
    p.executeCommand("APPEND", key, string(value))
    return p
}

func (p *Pipeline) Rpush(key string, value []byte) *Pipeline {
    p.executeCommand("RPUSH", key, string(value))
    return p
}

func (p *Pipeline) Lpush(key string, value []byte) *Pipeline {
    p.executeCommand("LPUSH", key, string(value))
    return p
}

func (p *Pipeline) Llen(key string) *Pipeline {
    p.executeCommand("LLEN", key)
    return p
}

func (p *Pipeline) Lrange(key string, start, end int) *Pipeline {
    p.executeCommand("LRANGE", key, strconv.Itoa(start), strconv.Itoa(end))
    return p
}

func (p *Pipeline) Ltrim(key string, start, end int) *Pipeline {
    p.executeCommand("LTRIM", key, strconv.Itoa(start), strconv.Itoa(end))
    return p
}

func (p *Pipeline) Lindex(key string, index int) *Pipeline {
    p.executeCommand("LINDEX", key, strconv.Itoa(index))
    return p
}

func (p *Pipeline) Lset(key string, index int, value []byte) *Pipeline {
    p.executeCommand("LSET", key, strconv.Itoa(index), string(value))
    return p
}

func (p *Pipeline) Lrem(key string, count int, value []byte) *Pipeline {
    p.executeCommand("LREM", key, strconv.Itoa(count), string(value))
    return p
}

func (p *Pipeline) Lpop(key string) *Pipeline {
    p.executeCommand("LPOP", key)
    return p
}

func (p *Pipeline) Rpop(key string) *Pipeline {
    p.executeCommand("RPOP", key)
    return p
}

func (p *Pipeline) Rpoplpush(key string, anKey string) *Pipeline {
    p.executeCommand("RPOPLPUSH", key, anKey)
    return p
}

func (p *Pipeline) Hset(key string, field string, value []byte) *Pipeline {
    p.executeCommand("HSET", key, field, string(value))
    return p
}

func (p *Pipeline) Hget(key string, field string) *Pipeline {
    p.executeCommand("HGET", key, field)
    return p
}

func (p *Pipeline) Hgetall(key string) *Pipeline {
    p.executeCommand("HGETALL", key)
    return p
}

func (p *Pipeline) Hdel(key string, field string) *Pipeline {
    p.executeCommand("HDEL", key, field)
    return p
}

func (p *Pipeline) Hlen(key string) *Pipeline {
    p.executeCommand("HLEN", key)
    return p
}

func (p *Pipeline) Hkeys(key string) *Pipeline {
    p.executeCommand("HKEYS", key)
    return p
}

func (p *Pipeline) Hvals(key string) *Pipeline {
    p.executeCommand("HVALS", key)
    return p
}

var _ = fmt.Println

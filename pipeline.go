package redis

import (
    "fmt"
    "io"
    "net"
    "bufio"
    "reflect"
    "strings"
    "strconv"
)

func (client *Client) Pipeline() *Pipeline {
    p := &Pipeline{
        client: client,
        stack : make([]operand, 0),
    }
    p.initCalls()
    return p
}

type operand struct {
    command string
    args []string
}

type Pipeline struct {
    client *Client
    stack []operand
    
    Exists func(string) *Pipeline
    Del func(string) *Pipeline
    Type func(string) *Pipeline
    Keys func(string) *Pipeline
    Expire func(string, int64) *Pipeline
    Set func(string, []byte) *Pipeline
    Get func(string) *Pipeline
    Incr func(string) *Pipeline
    Incrby func(string, int64) *Pipeline
    Decr func(string) *Pipeline
    Decrby func(string, int64) *Pipeline
    Append func(string, []byte) *Pipeline

    Rpush func(string, []byte) *Pipeline
    Lpush func(string, []byte) *Pipeline
    Llen func(string) *Pipeline
    Lrange func(string, int, int) *Pipeline
    Ltrim func(string, int, int) *Pipeline
    Lindex func(string, int) *Pipeline
    Lset func(string, int, []byte) *Pipeline
    Lrem func(string, int, []byte) *Pipeline
    Lpop func(string) *Pipeline
    Rpop func(string) *Pipeline
    Rpoplpush func(string, string) *Pipeline

    Hset func(string, string, []byte) *Pipeline
    Hget func(string, string) *Pipeline
    Hgetall func(string) *Pipeline
    Hdel func(string, string) *Pipeline
    Hlen func(string) *Pipeline
    Hkeys func(string) *Pipeline
    Hvals func(string) *Pipeline
}

func (p *Pipeline) initCalls() {
    p.initCall(&p.Exists, "exists")
    p.initCall(&p.Del, "del")
    p.initCall(&p.Type, "type")
    p.initCall(&p.Keys, "keys")
    p.initCall(&p.Expire, "expire")    
    p.initCall(&p.Get, "get")
    p.initCall(&p.Set, "set")
    p.initCall(&p.Incr, "incr")
    p.initCall(&p.Incrby, "incrby")
    p.initCall(&p.Decr, "decr")
    p.initCall(&p.Decrby, "decrby")
    p.initCall(&p.Append, "append")

    p.initCall(&p.Rpush, "rpush")
    p.initCall(&p.Lpush, "lpush")
    p.initCall(&p.Llen, "llen")
    p.initCall(&p.Lrange, "lrange")
    p.initCall(&p.Ltrim, "ltrim")
    p.initCall(&p.Lindex, "lindex")
    p.initCall(&p.Lset, "lset")
    p.initCall(&p.Lrem, "lrem")
    p.initCall(&p.Lpop, "lpop")
    p.initCall(&p.Rpop, "rpop")
    p.initCall(&p.Rpoplpush, "rpoplpush")

    p.initCall(&p.Hset, "hset")
    p.initCall(&p.Hget, "hget")
    p.initCall(&p.Hgetall, "hgetall")
    p.initCall(&p.Hdel, "hdel")
    p.initCall(&p.Hlen, "hlen")
    p.initCall(&p.Hkeys, "hkeys")
    p.initCall(&p.Hvals, "hvals")
}

func (p *Pipeline) initCall(fptr interface{}, cmd string) {
    fn := reflect.ValueOf(fptr).Elem()
    innerFunc := func(in []reflect.Value) []reflect.Value {
        args := make([]string, len(in))
        for i := range in {
            switch in[i].Kind() {
            case reflect.String:
                args[i] = in[i].String()
            case reflect.Int, reflect.Int64:
                args[i] = strconv.FormatInt(in[i].Int(), 10)
            case reflect.Slice:
                typ := in[i].Type()
                if typ.Elem().Kind() == reflect.Uint8 {
                    args[i] = string(in[i].Interface().([]byte))
                }
            }
        }
        p.executeCommand(strings.ToUpper(cmd), args...)
        return []reflect.Value{reflect.ValueOf(p)}
    }
    v := reflect.MakeFunc(fn.Type(), innerFunc)
    fn.Set(v)
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
    reader := bufio.NewReader(c)
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
    c, err := client.popCon()
    if err != nil {
        println(err.Error())
        goto End
    }

    data, err = p.rawSend(c, cmds)
    if err == io.EOF {
        c, err = client.openConnection()
        if err != nil {
            println(err.Error())
            goto End
        }

        data, err = p.rawSend(c, cmds)
    }

End:
    client.pushCon(c)
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

var _ = fmt.Println

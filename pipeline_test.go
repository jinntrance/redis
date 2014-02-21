package redis

import (
    "testing"
    "fmt"
)

func TestPipelineBasic(t *testing.T) {
    pipeline := client.Pipeline()
    pipeline.Set("p_test1", []byte("1"))
    pipeline.Set("p_test2", []byte("2"))
    pipeline.Expire("p_test1", 10)
    pipeline.Incr("p_test2")
    pipeline.Get("p_test1").Get("p_test2").Get("p_test3")

    pipeline.Rpush("pl_test1", []byte("1")).Lpush("pl_test1", []byte("2"))
    pipeline.Llen("pl_test1").Lrange("pl_test1", 0, -1).Lindex("pl_test1", 1)
    pipeline.Lpop("pl_test1").Rpop("pl_test1").Lrange("pl_test1", 0, -1)

    pipeline.Hset("ph_test1", "f1", []byte("1"))
    pipeline.Hset("ph_test1", "f2", []byte("2"))
    pipeline.Hset("ph_test2", "f1", []byte("1"))
    pipeline.Hset("ph_test2", "f2", []byte("2"))
    pipeline.Hget("ph_test1", "f1").Hget("ph_test2", "f2")
    pipeline.Hgetall("ph_test1").Hgetall("ph_test2")
    pipeline.Hkeys("ph_test1").Hkeys("ph_test2")
    pipeline.Hvals("ph_test1").Hvals("ph_test2")
    pipeline.Hlen("ph_test1").Hlen("ph_test2")
    pipeline.Hdel("ph_test1", "f1").Hdel("ph_test2", "f1")
    pipeline.Hget("ph_test1", "f1").Hget("ph_test2", "f1")

    data, err := pipeline.Execute()
    if err != nil {
        fmt.Println(err)
    }
    for i := range data {
        if err, ok := data[i].(error); ok {
            fmt.Println("Got Error:", err)
        } else {
            fmt.Println("Got:", data[i])
        }
    }
}

var _ = fmt.Println

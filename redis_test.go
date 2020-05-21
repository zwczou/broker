package broker

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Info struct {
	Num   int
	Value string
}

func TestRedis(t *testing.T) {
	pool := &redis.Pool{
		// Other pool configuration not shown in this example.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	broker := NewRedisBroker(pool)
	go broker.SubscribeLoop(func(e *Event) error {
		var info Info
		e.Unmarshal(&info)
		println("===", info.Num)
		return nil
	}, "test.0")

	go func() {
		for i := 0; i <= 100; i++ {
			var info Info
			info.Num = i
			info.Value = "info"
			broker.Publish("test.0", info)
			time.Sleep(time.Second * 1)
		}
	}()

	select {}
}

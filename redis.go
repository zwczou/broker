// https://github.com/micro/go-plugins/blob/master/broker/redis/redis.go
package broker

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v4"
)

type Event struct {
	Topic string
	Body  []byte
}

func (e *Event) Unmarshal(out interface{}) error {
	return msgpack.Unmarshal(e.Body, out)
}

type Handler func(*Event) error

type RedisSubscriber struct {
	*redis.PubSubConn
	handler Handler
}

func (s *RedisSubscriber) recv() {
	defer s.Close()

	for {
		switch x := s.PubSubConn.Receive().(type) {
		case redis.Message:
			var e Event
			e.Body = x.Data
			e.Topic = x.Channel

			if err := s.handler(&e); err != nil {
				logrus.WithField("channel", x.Channel).WithError(err).Error("handler error")
				break
			}
		case redis.Subscription:
			if x.Count == 0 {
				return
			}
		case error:
			return
		}
	}
}

func (s *RedisSubscriber) IsClosed() bool {
	return s.Conn.Err() != nil
}

type RedisBroker struct {
	pool *redis.Pool
}

func NewRedisBroker(pool *redis.Pool) *RedisBroker {
	return &RedisBroker{pool: pool}
}

func (b *RedisBroker) Publish(topic, msg interface{}) error {
	v, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}

	conn := b.pool.Get()
	_, err = redis.Int(conn.Do("PUBLISH", topic, v))
	conn.Close()
	return err
}

func (b *RedisBroker) Subscribe(handler Handler, topics ...interface{}) (*RedisSubscriber, error) {
	pubsub := &redis.PubSubConn{Conn: b.pool.Get()}
	rs := &RedisSubscriber{
		PubSubConn: pubsub,
		handler:    handler,
	}
	go rs.recv()

	if err := rs.Subscribe(topics...); err != nil {
		return nil, err
	}
	return rs, nil
}

func (b *RedisBroker) PSubscribe(handler Handler, topics ...interface{}) (*RedisSubscriber, error) {
	pubsub := &redis.PubSubConn{Conn: b.pool.Get()}
	rs := &RedisSubscriber{
		PubSubConn: pubsub,
		handler:    handler,
	}
	go rs.recv()

	if err := rs.PSubscribe(topics...); err != nil {
		return nil, err
	}
	return rs, nil
}

// 可以用退避算法
// 此处为了简化
func (b *RedisBroker) SubscribeLoop(handler Handler, topics ...interface{}) {
	for {
		sub, err := b.Subscribe(handler, topics...)
		if err != nil {
			logrus.WithError(err).Error("subscribe error")
			time.Sleep(time.Second)
			continue
		}

		for !sub.IsClosed() {
			time.Sleep(time.Millisecond * 200)
		}
	}
}

func (b *RedisBroker) PSubscribeLoop(handler Handler, topics ...interface{}) {
	for {
		sub, err := b.PSubscribe(handler, topics...)
		if err != nil {
			logrus.WithError(err).Error("psubscribe error")
			time.Sleep(time.Second)
			continue
		}

		for !sub.IsClosed() {
			time.Sleep(time.Millisecond * 200)
		}
	}
}

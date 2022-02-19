package CivetRedis

import (
	"CivetRedis/CivetRedis/BaseInstance"
	"context"
	"fmt"
	"github.com/go-redis/redis"
)

type Producer struct {
	Ins    *BaseInstance.Redis
	Group  string
	Stream string
}

func CreateProducer(conf *BaseInstance.RedisBaseConfig, Group, Stream string) *Producer {
	Cli, err := BaseInstance.ConnectServer(conf)
	BaseInstance.FailChecker(err, "Create Fail")
	return &Producer{
		Ins:    Cli,
		Group:  Group,
		Stream: Stream,
	}
}

func (p *Producer) SendToQueue(ctx context.Context, key string, msg string) {
	arg := redis.XAddArgs{
		Stream:     p.Stream,
		NoMkStream: false,
		MaxLen:     20,
		ID:         "*",
		Values:     []string{key, msg},
	}
	add, err := p.Ins.Client.XAdd(ctx, &arg).Result()
	BaseInstance.FailChecker(err, "XADD Fail")
	fmt.Println(add)
}

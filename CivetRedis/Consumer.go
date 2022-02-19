package CivetRedis

import (
	"CivetRedis/CivetRedis/BaseInstance"
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

type Consumer struct {
	Ins    *BaseInstance.Redis
	Group  string
	Stream string
}

func CreateConsumer(conf *BaseInstance.RedisBaseConfig, Group string, Stream string) *Consumer {
	Cli, err := BaseInstance.ConnectServer(conf)
	BaseInstance.FailChecker(err, "Create Fail")
	return &Consumer{
		Ins:    Cli,
		Group:  Group,
		Stream: Stream,
	}
}

func (c *Consumer) CreateGroup(ctx context.Context, ID string) {
	result, err := c.Ins.Client.XGroupCreate(ctx, c.Stream, c.Group, ID).Result()
	BaseInstance.FailChecker(err, "CreateGroup Fail")
	fmt.Println(result)
}
func (c *Consumer) CreateGroupConsumer(ctx context.Context, consumerName string) {
	result, err := c.Ins.Client.XGroupCreateConsumer(ctx, c.Stream, c.Group, consumerName).Result()
	BaseInstance.FailChecker(err, "Create Fail")
	fmt.Println(result)
}
func (c *Consumer) ReadFromGroupQueue(ctx context.Context, consumerName string, count int64, block time.Duration) {
	arg := redis.XReadGroupArgs{
		Group:    c.Group,
		Consumer: consumerName,
		Streams:  []string{c.Stream, ">"},
		Count:    count,
		Block:    block,
		NoAck:    false,
	}
	result, err := c.Ins.Client.XReadGroup(ctx, &arg).Result()
	BaseInstance.FailChecker(err, "Read From Group fail")
	fmt.Println(result)
}
func (c *Consumer) ReadFromQueue(ctx context.Context, number int64, block time.Duration) {
	arg := redis.XReadArgs{
		Streams: []string{c.Stream, "$"},
		Count:   number,
		Block:   block,
	}
	result, err := c.Ins.Client.XRead(ctx, &arg).Result()
	BaseInstance.FailChecker(err, "Read Fail")
	fmt.Println(result)

}

func (c *Consumer) DelMsgFromQueue(ctx context.Context, ID string) {
	res, err := c.Ins.Client.XDel(ctx, c.Stream, ID).Result()
	BaseInstance.FailChecker(err, "Del Fail")
	fmt.Println(res)
}
func (c *Consumer) GetQueueLen(ctx context.Context) {
	result, err := c.Ins.Client.XLen(ctx, c.Stream).Result()
	BaseInstance.FailChecker(err, "get len fail")
	fmt.Println(result)
}
func (c *Consumer) GetQueueByRange(ctx context.Context, startID string, endID string) {
	result, err := c.Ins.Client.XRange(ctx, c.Stream, startID, endID).Result()
	BaseInstance.FailChecker(err, "getQueueByRange Fail")
	fmt.Println(result)
}
func (c *Consumer) CutQueue(ctx context.Context, maxLen int64) {
	result, err := c.Ins.Client.XTrimMaxLen(ctx, c.Stream, maxLen).Result()
	BaseInstance.FailChecker(err, "cut Queue")
	fmt.Println(result)
}

package main

import (
	"CivetRedis/CivetRedis"
	"CivetRedis/CivetRedis/BaseInstance"
	"context"
)

func main() {
	ctx := context.Background()
	redisconf := BaseInstance.RedisBaseConfig{
		Host:     "127.0.0.1",
		Port:     "6379",
		Password: "",
		UserName: "",
		Db:       0,
		Size:     1,
	}
	Newproducer := CivetRedis.CreateProducer(&redisconf, "23", "test2")
	Newproducer.SendToQueue(ctx, "message", "fuck")

	newCumser := CivetRedis.CreateConsumer(&redisconf, "23", "test2")
	newCumser.CreateGroup(ctx, "$")
	newCumser.CreateGroupConsumer(ctx, "email")
	for true {
		newCumser.ReadFromGroupQueue(ctx, "email", 1, 1000)
	}

}

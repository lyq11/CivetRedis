package BaseInstance

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
)

type RedisBaseConfig struct {
	Host     string
	Port     string
	Password string
	UserName string
	Db       int
	Size     int
}
type Redis struct {
	Client *redis.Client
	Conf   *RedisBaseConfig
}

func ConnectServer(conf *RedisBaseConfig) (*Redis, error) {
	newClient := &Redis{Conf: conf}
	newClient.Client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", newClient.Conf.Host, newClient.Conf.Port),
		Username: newClient.Conf.UserName,
		Password: newClient.Conf.Password,
		DB:       newClient.Conf.Db,
		PoolSize: newClient.Conf.Size,
	})
	_, err := newClient.Client.Ping(context.TODO()).Result()
	if err != nil {
		return newClient, err
	}
	return newClient, nil
}
func FailChecker(err error, msg string) {
	if err != nil {
		fmt.Println(err.Error())
		fmt.Println(msg)
	}
}

package redisq

import (
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	errNoMessage = errors.New("no message need handle")
	errMetadata  = errors.New("metadata invalid")
)

type partition struct {
	id       int
	rds      redis.UniversalClient
	prefix   string
	delay    string
	ready    string
	unCommit string
}

func newPartition(id int, keyPrefix string, rds redis.UniversalClient) partition {
	prefix := fmt.Sprintf("{%s:%d}:", keyPrefix, id)
	return partition{
		id:       id,
		rds:      rds,
		prefix:   prefix,
		delay:    prefix + "delay",
		ready:    prefix + "ready",
		unCommit: prefix + "unCommit",
	}
}

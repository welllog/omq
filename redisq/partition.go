package redisq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/welllog/omq"
)

var errNoMessage = errors.New("no message need handle")

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

func (p *partition) size(ctx context.Context) (int, error) {
	return _sizeCmd.Run(ctx, p.rds, []string{p.ready, p.delay, p.unCommit}).Int()
}

func (p *partition) pushDelay(ctx context.Context, msgID string, msg *omq.Message, ttl int64) error {
	enc, err := encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("encode msg failed: %w", err)
	}

	return _pushDelayCmd.Run(ctx, p.rds, []string{p.delay, p.prefix + msgID},
		msg.DelayAt.Unix(), msgID, enc, msg.MaxRetry, ttl).Err()
}

func (p *partition) pushReady(ctx context.Context, msgID string, msg *omq.Message, ttl int64) error {
	enc, err := encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("encode msg failed: %w", err)
	}

	return _pushReadyCmd.Run(ctx, p.rds, []string{p.ready, p.prefix + msgID},
		msgID, enc, msg.MaxRetry, ttl).Err()
}

func (p *partition) delayToReady(ctx context.Context, delayAt, num int64) (int, error) {
	return _delayToReadyCmd.Run(ctx, p.rds, []string{p.delay, p.ready}, delayAt, num).Int()
}

func (p *partition) clear(ctx context.Context) error {
	return p.rds.Del(ctx, p.ready, p.delay, p.unCommit).Err()
}

func (p *partition) fetchReady(ctx context.Context) (*omq.Message, error) {
	arr, err := _fetchReadyCmd.Run(ctx, p.rds, []string{p.ready, p.unCommit}, p.prefix, time.Now().Unix()).StringSlice()
	if err != nil {
		return nil, err
	}

	if len(arr) != 3 {
		return nil, errNoMessage
	}

	var msg omq.Message
	if err := decodeMsg(arr[1], &msg); err != nil {
		return nil, err
	}

	retry, _ := strconv.Atoi(arr[2])
	msg.Metadata = msgMeta{
		partition: p.id,
		msgId:     arr[0],
		retry:     retry,
	}
	return &msg, nil
}

func (p *partition) commitTimeoutToReady(ctx context.Context, timeoutAt int64, num int) (int, error) {
	return _unCommitToReadyCmd.Run(ctx, p.rds, []string{p.unCommit, p.ready}, timeoutAt, num, p.prefix).Int()
}

func (p *partition) delMsg(ctx context.Context, msgID string) error {
	return p.rds.Del(ctx, p.prefix+msgID).Err()
}

func (p *partition) commitMsg(ctx context.Context, msgID string) error {
	return p.rds.ZRem(ctx, p.unCommit, msgID).Err()
}

func (p *partition) commitAndDelMsg(ctx context.Context, msgID string) error {
	return _removeMsgCmd.Run(ctx, p.rds, []string{p.unCommit, p.prefix + msgID}, msgID).Err()
}

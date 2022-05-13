package redisq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/welllog/omq"
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

func (p *partition) size(ctx context.Context) (int, error) {
	return p.rds.Eval(ctx, _SIZE, []string{
		p.ready, p.delay, p.unCommit,
	}).Int()
}

func (p *partition) pushDelay(ctx context.Context, msg *omq.Message, ttl int64) error {
	enc, err := encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("encode msg failed: %w", err)
	}

	return p.rds.Eval(
		ctx,
		_PUSH_DELAY,
		[]string{p.delay, p.prefix + msg.ID},
		msg.DelayAt.Unix(),
		msg.ID,
		ttl,
		enc,
	).Err()
}

func (p *partition) pushReady(ctx context.Context, msg *omq.Message, ttl int64) error {
	enc, err := encodeMsg(msg)
	if err != nil {
		return fmt.Errorf("encode msg failed: %w", err)
	}

	return p.rds.Eval(
		ctx,
		_PUSH_READY,
		[]string{p.ready, p.prefix + msg.ID},
		msg.ID,
		ttl,
		enc,
	).Err()
}

func (p *partition) delayToReady(ctx context.Context, delayAt int64) error {
	return p.rds.Eval(
		ctx,
		_DELAY_TO_READY,
		[]string{p.delay, p.ready},
		delayAt,
	).Err()
}

func (p *partition) clear(ctx context.Context) error {
	return p.rds.Del(ctx, p.ready, p.delay, p.unCommit).Err()
}

func (p *partition) commitTimeoutToReady(ctx context.Context, timeoutAt int64) error {
	return p.rds.Eval(
		ctx,
		_COMMIT_TO_READY,
		[]string{p.unCommit, p.ready},
		timeoutAt,
		10,
		p.prefix,
	).Err()
}

func (p *partition) bPopReady(ctx context.Context, timeout time.Duration) ([]*omq.Message, error) {
	arr, err := p.rds.BLPop(ctx, timeout, p.ready).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("blpop failed: %w", err)
	}

	if len(arr) < 2 {
		return nil, nil
	}

	id := arr[1]
	msgKey := p.prefix + id
	val, err := p.rds.Get(ctx, msgKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("get failed: %w", err)
	}

	msgs := []*omq.Message{
		{
			ID:       id,
			Metadata: p.id,
		},
	}
	if err := decodeMsg(val, msgs[0]); err != nil {
		return nil, err
	}
	return msgs, nil
}

func (p *partition) popReady(ctx context.Context, limit int) ([]*omq.Message, error) {
	arr, err := p.rds.Eval(
		ctx,
		_POP_READY,
		[]string{p.ready},
		limit-1,
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("pop ready failed: %w", err)
	}

	count := len(arr)
	if count == 0 {
		return nil, nil
	}
	msgs := make([]*omq.Message, 0, count)
	msgKeys := make([]string, count)
	for i, id := range arr {
		msgKeys[i] = p.prefix + id
	}

	vals, err := p.rds.MGet(ctx, msgKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget failed: %w", err)
	}

	for i, v := range vals {
		if v == nil {
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}

		msg := omq.Message{
			ID:       arr[i],
			Metadata: p.id,
		}
		if err := decodeMsg(str, &msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, nil
}

func (p *partition) safePopReady(ctx context.Context, limit int) ([]*omq.Message, error) {
	arr, err := p.rds.Eval(
		ctx,
		_SAFE_POP_READY,
		[]string{p.ready, p.unCommit},
		limit-1,
		time.Now().Unix(),
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("pop ready failed: %w", err)
	}

	count := len(arr)
	if count == 0 {
		return nil, nil
	}

	msgs := make([]*omq.Message, 0, count)
	msgKeys := make([]string, 0, count)
	for _, v := range arr {
		msgKeys = append(msgKeys, p.prefix+v)
	}

	values, err := p.rds.MGet(ctx, msgKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget failed: %w", err)
	}

	for i, v := range values {
		if v == nil {
			continue
		}

		str, ok := v.(string)
		if !ok {
			continue
		}

		msg := omq.Message{
			ID:       arr[i],
			Metadata: p.id,
		}
		if err := decodeMsg(str, &msg); err != nil {
			return nil, err
		}

		msgs = append(msgs, &msg)
	}
	return msgs, nil
}

func (p *partition) delMsg(ctx context.Context, msgID string) error {
	return p.rds.Del(ctx, p.prefix+msgID).Err()
}

func (p *partition) commitMsg(ctx context.Context, msgID string) error {
	return p.rds.ZRem(ctx, p.unCommit, msgID).Err()
}

func (p *partition) commitAndDelMsg(ctx context.Context, msgID string) error {
	return p.rds.Eval(ctx, _REMOVE_MSG, []string{p.unCommit, p.prefix + msgID}, msgID).Err()
}

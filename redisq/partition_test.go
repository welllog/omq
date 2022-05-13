package redisq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/welllog/omq"
)

var _rds redis.UniversalClient

func TestMain(m *testing.M) {
	_rds = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})
	m.Run()
	_rds.Close()
}

func initPartition() *partition {
	p := newPartition(0, "test_partition", _rds)
	return &p
}

func TestPartitionPushDelay(t *testing.T) {
	p := initPartition()

	ctx := context.Background()
	now := time.Now()
	msg := omq.Message{
		ID:      "1",
		Topic:   "topic",
		Payload: omq.ByteEncoder("payload"),
		DelayAt: now.Add(time.Second),
	}
	err := p.pushDelay(ctx, &msg, 60)

	if err != nil {
		t.Fatalf("\t%s Push delay should success: %v", failed, err)
	}

	score, err := p.rds.ZScore(ctx, "{test_partition:0}:delay", "1").Result()
	if err != nil {
		t.Fatalf("\t%s Should get score: %v", failed, err)
	}
	if int64(score) != now.Unix()+1 {
		t.Fatalf("\t%s score = %v, want: %v", failed, score, now.Unix()+1)
	}

	payload, err := p.rds.Get(ctx, "{test_partition:0}:1").Result()
	if err != nil {
		t.Fatalf("\t%s Should get payload: %v", failed, err)
	}

	str, _ := encodeMsg(&msg)
	if payload != str {
		t.Fatalf("\t%s payload = %v, want: %v", failed, payload, str)
	}

	time.Sleep(time.Second)
	err = p.delayToReady(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("\t%s Push delay should success: %v", failed, err)
	}

	remNum, _ := p.rds.ZRem(ctx, "{test_partition:0}:delay", "1").Result()
	if remNum > 0 {
		t.Fatalf("\t%s remNum = %v, want: %v", failed, remNum, 0)
	}

	llen, _ := p.rds.LLen(ctx, "{test_partition:0}:ready").Result()
	if llen != 1 {
		t.Fatalf("\t%s llen = %v, want: %v", failed, llen, 1)
	}

	msgs, err := p.bPopReady(ctx, time.Second)
	if err != nil {
		t.Fatalf("\t%s Pop ready should success: %v", failed, err)
	}
	if len(msgs) != 1 {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, len(msgs), 1)
	}

	p.clear(ctx)

	t.Logf("\t%s Push delay success", succeed)
}

func TestPartitionPushReady(t *testing.T) {
	p := initPartition()

	ctx := context.Background()
	msg := omq.Message{
		ID:      "2",
		Topic:   "topic",
		Payload: omq.ByteEncoder("payload"),
	}

	err := p.pushReady(ctx, &msg, 60)
	if err != nil {
		t.Fatalf("\t%s Push ready should success: %v", failed, err)
	}

	llen, _ := p.rds.LLen(ctx, "{test_partition:0}:ready").Result()
	if llen != 1 {
		t.Fatalf("\t%s llen = %v, want: %v", failed, llen, 1)
	}

	msgs, _ := p.popReady(ctx, 2)
	if len(msgs) != 1 {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, len(msgs), 1)
	}

	err = p.delMsg(ctx, msgs[0].ID)
	if err != nil {
		t.Fatalf("\t%s del msg: %v", failed, err)
	}

	p.clear(ctx)

	t.Logf("\t%s Push ready success", succeed)
}

func TestPartitionSafePopReady(t *testing.T) {
	p := initPartition()

	ctx := context.Background()
	msg := omq.Message{
		ID:      "3",
		Topic:   "topic",
		Payload: omq.ByteEncoder("payload"),
	}

	err := p.pushReady(ctx, &msg, 60)
	if err != nil {
		t.Fatalf("\t%s Push ready should success: %v", failed, err)
	}

	msgs, _ := p.safePopReady(ctx, 5)
	if len(msgs) != 1 {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, len(msgs), 1)
	}
	num, _ := p.rds.ZCard(ctx, "{test_partition:0}:unCommit").Result()
	if num != 1 {
		t.Fatalf("\t%s unCommit num = %v, want: %v", failed, num, 1)
	}

	time.Sleep(time.Second)
	err = p.commitTimeoutToReady(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("\t%s commit timeout should success: %v", failed, err)
	}

	readyNum, _ := p.rds.LLen(ctx, "{test_partition:0}:ready").Result()
	if readyNum != 1 {
		t.Fatalf("\t%s ready num = %v, want: %v", failed, readyNum, 1)
	}
	msgs, _ = p.safePopReady(ctx, 1)
	if len(msgs) != 1 {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, len(msgs), 1)
	}

	err = p.commitAndDelMsg(ctx, msgs[0].ID)
	if err != nil {
		t.Fatalf("\t%s commitAndDelMsg should success: %v", failed, err)
	}

	num, _ = p.rds.ZCard(ctx, "{test_partition:0}:unCommit").Result()
	if num != 0 {
		t.Fatalf("\t%s unCommit num = %v, want: %v", failed, num, 0)
	}

	err = p.rds.Get(ctx, "{test_partition:0}:3").Err()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("\t%s msg should be deleted: %v", failed, err)
	}

	p.clear(ctx)

	t.Logf("\t%s Safe pop ready success", succeed)
}

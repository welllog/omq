package redisq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

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
		ID:       "1",
		Topic:    "topic",
		Payload:  omq.ByteEncoder("payload"),
		DelayAt:  now.Add(time.Second),
		MaxRetry: 1,
	}
	msgID := omq.UUID()
	err := p.pushDelay(ctx, msgID, &msg, 60)

	if err != nil {
		t.Fatalf("\t%s Push delay should success: %v", failed, err)
	}

	score, err := p.rds.ZScore(ctx, p.delay, msgID).Result()
	if err != nil {
		t.Fatalf("\t%s Should get score: %v", failed, err)
	}
	if int64(score) != now.Unix()+1 {
		t.Fatalf("\t%s score = %v, want: %v", failed, score, now.Unix()+1)
	}

	payload, err := p.rds.HGet(ctx, p.prefix+msgID, "msg").Result()
	if err != nil {
		t.Fatalf("\t%s Should get payload: %v", failed, err)
	}

	str, _ := encodeMsg(&msg)
	if payload != str {
		t.Fatalf("\t%s payload = %v, want: %v", failed, payload, str)
	}

	time.Sleep(time.Second)
	num, err := p.delayToReady(ctx, time.Now().Unix(), 5)
	if err != nil {
		t.Fatalf("\t%s Push delay should success: %v", failed, err)
	}
	if num != 1 {
		t.Fatalf("\t%s to ready task num: %d, want: %d", failed, num, 1)
	}

	n, err := p.rds.ZCard(ctx, p.delay).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n > 0 {
		t.Fatalf("\t%s delay queue len = %d, want: %d", failed, n, 0)
	}

	n, err = p.rds.LLen(ctx, p.ready).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("\t%s ready queue len = %v, want: %v", failed, n, 1)
	}

	fMsg, err := p.fetchReady(ctx)
	if err != nil {
		t.Fatalf("\t%s Pop ready should success: %v", failed, err)
	}
	if fMsg == nil {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, 0, 1)
	}

	n, err = p.rds.LLen(ctx, p.ready).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("\t%s ready queue len = %v, want: %v", failed, n, 0)
	}

	n, _ = p.rds.ZCard(ctx, p.unCommit).Result()
	if n != 1 {
		t.Fatalf("\t%s uncommit queue len = %v, want: %v", failed, n, 1)
	}

	err = p.commitAndDelMsg(ctx, msgID)
	if err != nil {
		t.Fatal(err)
	}
	n, err = p.rds.ZCard(ctx, p.unCommit).Result()
	if err != nil {
		t.Fatal(err)
	}

	if n > 0 {
		t.Fatalf("\t%s ready queue len = %v, want: %v", failed, n, 0)
	}

	n, err = p.rds.Exists(ctx, p.prefix+msgID).Result()
	if err != nil {
		t.Fatal(err)
	}

	if n > 0 {
		t.Fatalf("\t%s ready queue len = %v, want: %v", failed, n, 0)
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
	msgID := omq.UUID()
	err := p.pushReady(ctx, msgID, &msg, 60)
	if err != nil {
		t.Fatalf("\t%s Push ready should success: %v", failed, err)
	}

	llen, _ := p.rds.LLen(ctx, p.ready).Result()
	if llen != 1 {
		t.Fatalf("\t%s ready queue len = %v, want: %v", failed, llen, 1)
	}

	fMsg, _ := p.fetchReady(ctx)
	if fMsg == nil {
		t.Fatalf("\t%s ready msg num = %v, want: %v", failed, 0, 1)
	}

	n, _ := p.rds.ZCard(ctx, p.unCommit).Result()
	if n != 0 {
		t.Fatalf("\t%s uncommit queue len = %v, want: %v", failed, n, 0)
	}

	err = p.delMsg(ctx, msgID)
	if err != nil {
		t.Fatalf("\t%s del msg: %v", failed, err)
	}

	p.clear(ctx)

	t.Logf("\t%s Push ready success", succeed)
}

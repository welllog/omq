package redisq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/welllog/omq"
)

func TestQueueProduce(t *testing.T) {
	q := NewQueue(_rds, "test:queue", WithPartitionNum(2), WithDelMsgOnCommit(), WithMsgTTL(20), WithMaxRetry(0))

	ctx, cancel := context.WithCancel(context.Background())

	now := time.Now()
	msgs := []*omq.Message{
		{
			Topic:    "ready1",
			Payload:  omq.ByteEncoder("words1"),
			MaxRetry: 1,
		},
		{
			Topic:    "ready2",
			Payload:  omq.ByteEncoder("words2"),
			MaxRetry: 1,
		},
		{
			Topic:   "ready3",
			Payload: omq.ByteEncoder("words3"),
		},
		{
			Topic:   "delay1",
			Payload: omq.ByteEncoder("words4"),
			DelayAt: now.Add(time.Second),
		},
		{
			Topic:   "delay2",
			Payload: omq.ByteEncoder("words5"),
			DelayAt: now.Add(time.Second),
		},
		{
			Topic:   "delay3",
			Payload: omq.ByteEncoder("words6"),
			DelayAt: now.Add(time.Second),
		},
	}

	for _, msg := range msgs {
		q.Produce(ctx, msg)
	}

	fetcher, err := q.Fetcher(ctx, 2)
	if err != nil {
		t.Fatalf("\t%s fetcher get: %v", failed, err)
	}

	go func() {
		time.Sleep(1500 * time.Millisecond)
		cancel()
	}()

	for msg := range fetcher.Messages() {
		fetcher.Commit(ctx, msg)
		fmt.Println(msg)
	}

	q.Clear(context.TODO())
}

func TestQueueCommitTimeOut(t *testing.T) {
	q := NewQueue(_rds, "test:queue", WithPartitionNum(2), WithDelMsgOnCommit(),
		WithMsgTTL(10), WithCommitTimeout(1))

	ctx, cancel := context.WithCancel(context.Background())

	q.Produce(ctx, &omq.Message{
		Topic:    "ready1",
		Payload:  omq.ByteEncoder("words1"),
		MaxRetry: 3,
	})

	fetcher, err := q.Fetcher(ctx, 2)
	if err != nil {
		t.Fatalf("\t%s fetcher get: %v", failed, err)
	}

	go func() {
		time.Sleep(1500 * time.Millisecond)
		cancel()
	}()

	now := time.Now()
	var i int
	for msg := range fetcher.Messages() {
		now1 := time.Now()
		fmt.Println(now1.Sub(now).Milliseconds())
		now = now1
		fmt.Println(msg)
		if i > 0 {
			fetcher.Commit(ctx, msg)
		}
		i++
	}

	q.Clear(context.TODO())
}

func TestWithPartitionOrder(t *testing.T) {
	q := NewQueue(_rds, "test:queue", WithPartitionNum(3), WithDelMsgOnCommit(),
		WithMsgTTL(10), WithCommitTimeout(1), WithPartitionOrder())

	ctx, cancel := context.WithCancel(context.Background())

	tests := []*omq.Message{
		{Topic: "t1", Payload: omq.ByteEncoder("words1")},
		{Topic: "t1", Payload: omq.ByteEncoder("words2")},
		{Topic: "t1", Payload: omq.ByteEncoder("words3")},
		{ID: "12345", Topic: "t2", Payload: omq.ByteEncoder("words4")},
		{ID: "12345", Topic: "t2", Payload: omq.ByteEncoder("words5")},
		{ID: "12345", Topic: "t2", Payload: omq.ByteEncoder("words6")},
	}
	for _, tt := range tests {
		if err := q.Produce(ctx, tt); err != nil {
			t.Fatal(err)
		}
	}

	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	fetcher, err := q.Fetcher(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	m := make(map[string]int)
	for msg := range fetcher.Messages() {
		if p, ok := m[msg.Topic]; ok {
			meta := msg.Metadata.(msgMeta)
			if p != meta.partition {
				t.Fatalf("\t%s partition = %v, want: %v", failed, meta.partition, p)
			} else {
				t.Logf("\t%s partition correct: %d", succeed, p)
			}
		} else {
			meta := msg.Metadata.(msgMeta)
			m[msg.Topic] = meta.partition
		}
	}

	q.Clear(context.TODO())
}

func TestWithSleepOnEmpty(t *testing.T) {
	q := NewQueue(_rds, "test:queue", WithPartitionNum(3), WithSleepOnEmpty(time.Second),
		WithPartitionOrder(), WithMaxRetry(0))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		go func() {
			for i := 0; i < 10; i++ {
				q.Produce(ctx, &omq.Message{ // partition 0
					ID:      "11123",
					Topic:   "test",
					Payload: omq.ByteEncoder("hello"),
				})
			}
		}()

		go func() {
			time.Sleep(60 * time.Millisecond)
			for i := 0; i < 10; i++ {
				q.Produce(ctx, &omq.Message{ // partition 1
					ID:      "2222",
					Topic:   "test2",
					Payload: omq.ByteEncoder("hello"),
				})
			}
		}()

		q.Produce(ctx, &omq.Message{ // partition 2
			ID:      "333",
			Topic:   "test3",
			Payload: omq.ByteEncoder("hello"),
		})
		time.Sleep(100 * time.Millisecond)
		q.Produce(ctx, &omq.Message{ // partition 2
			ID:      "333",
			Topic:   "test3",
			Payload: omq.ByteEncoder("hello"),
		})
		time.Sleep(time.Second)
		q.Produce(ctx, &omq.Message{ // partition 2
			ID:      "333",
			Topic:   "test3",
			Payload: omq.ByteEncoder("hello"),
		})

		time.Sleep(1500 * time.Millisecond)
		cancel()
	}()

	time.Sleep(50 * time.Millisecond)
	fetcher, err := q.Fetcher(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	var i int
	for msg := range fetcher.Messages() {
		meta := msg.Metadata.(msgMeta)
		if msg.Topic == "test" {
			if time.Since(now).Seconds() >= 1 {
				t.Log(meta.partition, "---", msg.ID, time.Since(now).Seconds())
				t.Fatal("sleep must less than 1 second")
			}
		} else if msg.Topic == "test2" {
			if time.Since(now).Seconds() < 1 {
				t.Log(meta.partition, "---", msg.ID, time.Since(now).Seconds())
				t.Fatal("sleep must more than 1 second")
			}
		} else {
			if i == 0 {
				if time.Since(now).Seconds() > 1 {
					t.Log(meta.partition, "---", msg.ID, time.Since(now).Seconds())
					t.Fatal("sleep must less than 1 second")
				}
			} else if i == 1 {
				if time.Since(now).Seconds() < 1 {
					t.Log(meta.partition, "---", msg.ID, time.Since(now).Seconds())
					t.Fatal("sleep must more than 1 second")
				}
			} else {
				if time.Since(now).Seconds() < 2 {
					t.Log(meta.partition, "---", msg.ID, time.Since(now).Seconds())
					t.Fatal("sleep must more than 2 second")
				}
			}
			i++
		}
		//t.Log(meta.partition, "---", msg.ID, "---", msg.Topic, time.Since(now).Seconds())
	}

	q.Clear(context.Background())
}

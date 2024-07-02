package redisq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/welllog/omq"
)

var _rds redis.UniversalClient

func TestMain(m *testing.M) {
	_rds = redis.NewUniversalClient(&redis.UniversalOptions{})
	m.Run()
	_ = _rds.Close()
}

func TestQueue_Produce(t *testing.T) {
	//q1 := NewQueue(_rds, "queue")
	//q2 := NewQueue(_rds, "queue-unique", WithPayloadUniqueOptimization())
	//q3 := NewQueue(_rds, "queue-delay", WithDisableReady())
	//q4 := NewQueue(_rds, "queue-ready", WithDisableDelay())
	//q5 := NewQueue(_rds, "queue-delay-unique", WithPayloadUniqueOptimization(), WithDisableReady())
	q6 := NewQueue(_rds, "queue-ready-unique", WithPayloadUniqueOptimization(), WithDisableDelay())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()

	//Nil(t, q1.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-1")}))
	//Nil(t, q1.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-2"), DelayAt: now.Add(10 * time.Second)}))
	//fmt.Println(q1.Size(ctx))
	//fetcher, err := q1.Fetcher(ctx, 2)
	//if err != nil {
	//	t.Fatalf("\t%s fetcher get: %v", failed, err)
	//}
	//
	//go func() {
	//	time.Sleep(11 * time.Second)
	//	cancel()
	//}()
	//for msg := range fetcher.Messages() {
	//	Nil(t, fetcher.Commit(ctx, msg))
	//	fmt.Printf("%+v \n", msg)
	//}
	//q1.Clear(context.Background())

	//Nil(t, q2.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-1")}))
	//Nil(t, q2.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-2"), DelayAt: now.Add(10 * time.Second)}))
	//fetcher, err := q2.Fetcher(ctx, 2)
	//if err != nil {
	//	t.Fatalf("\t%s fetcher get: %v", failed, err)
	//}
	//
	//go func() {
	//	time.Sleep(11 * time.Second)
	//	cancel()
	//}()
	//for msg := range fetcher.Messages() {
	//	Nil(t, fetcher.Commit(ctx, msg))
	//	fmt.Printf("%+v \n", msg)
	//}

	//Nil(t, q3.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-delay-1")}))
	//Nil(t, q3.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-delay-2"), DelayAt: now.Add(10 * time.Second)}))
	//fetcher, err := q3.Fetcher(ctx, 2)
	//if err != nil {
	//	t.Fatalf("\t%s fetcher get: %v", failed, err)
	//}
	//
	//go func() {
	//	time.Sleep(11 * time.Second)
	//	cancel()
	//}()
	//for msg := range fetcher.Messages() {
	//	Nil(t, fetcher.Commit(ctx, msg))
	//	fmt.Printf("%+v \n", msg)
	//}

	//Nil(t, q4.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-ready-1")}))
	//Nil(t, q4.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-ready-1"), DelayAt: now.Add(10 * time.Second)}))
	//fetcher, err := q4.Fetcher(ctx, 2)
	//if err != nil {
	//	t.Fatalf("\t%s fetcher get: %v", failed, err)
	//}
	//
	//go func() {
	//	time.Sleep(11 * time.Second)
	//	cancel()
	//}()
	//for msg := range fetcher.Messages() {
	//	Nil(t, fetcher.Commit(ctx, msg))
	//	fmt.Printf("%+v \n", msg)
	//}

	//Nil(t, q5.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-delay-1")}))
	//Nil(t, q5.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-delay-2"), DelayAt: now.Add(10 * time.Second)}))
	//fetcher, err := q5.Fetcher(ctx, 2)
	//if err != nil {
	//	t.Fatalf("\t%s fetcher get: %v", failed, err)
	//}
	//
	//go func() {
	//	time.Sleep(11 * time.Second)
	//	cancel()
	//}()
	//for msg := range fetcher.Messages() {
	//	Nil(t, fetcher.Commit(ctx, msg))
	//	fmt.Printf("%+v \n", msg)
	//}

	Nil(t, q6.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-ready-1")}))
	Nil(t, q6.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("words-queue-unique-ready-2"), DelayAt: now.Add(10 * time.Second)}))
	fetcher, err := q6.Fetcher(ctx, 2)
	if err != nil {
		t.Fatalf("\t%s fetcher get: %v", failed, err)
	}

	go func() {
		time.Sleep(22 * time.Second)
		cancel()
	}()
	for msg := range fetcher.Messages() {
		Nil(t, fetcher.Commit(ctx, msg))
		fmt.Printf("%+v \n", msg)
	}
}

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

	now := time.Now()
	fetcher, err := q.Fetcher(ctx, 2)
	if err != nil {
		t.Fatalf("\t%s fetcher get: %v", failed, err)
	}

	go func() {
		time.Sleep(10 * time.Second)
		cancel()
	}()

	var i int
	for msg := range fetcher.Messages() {
		now1 := time.Now()
		fmt.Println(now1.Sub(now).Milliseconds())
		now = now1
		fmt.Printf("%+v \n", msg)
		if i > 5 {
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

func Nil(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Fatalf("unexpected error: %v", err)
	}
}

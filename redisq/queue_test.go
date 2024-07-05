package redisq

import (
	"context"
	"fmt"
	"strconv"
	"sync"
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
	ttlOpt := WithMsgTTL(60)
	delOpt := WithDelMsgOnCommit()
	timeoutOpt := WithCommitTimeout(1)

	// --------------------q1
	q1 := NewQueue(_rds, "queue", ttlOpt, delOpt, timeoutOpt)
	testQueue(t, q1, false)
	testQueueCommitTimeOut(t, q1)

	// q2 ---------------------------
	q2 := NewQueue(_rds, "queue-unique", WithPayloadUniqueOptimization(), ttlOpt, timeoutOpt)
	testQueue(t, q2, true)
	testQueueCommitTimeOut(t, q2)

	// q3 ---------------------------
	q3 := NewQueue(_rds, "queue-delay", WithDisableReady(), ttlOpt, delOpt, timeoutOpt)
	testDelayQueue(t, q3, false)
	testQueueCommitTimeOut(t, q3)

	// q4 ---------------------------
	q4 := NewQueue(_rds, "queue-ready", WithDisableDelay(), ttlOpt, delOpt, timeoutOpt)
	testReadyQueue(t, q4, false)
	testQueueCommitTimeOut(t, q4)

	// q5 ---------------------------
	q5 := NewQueue(_rds, "queue-delay-unique", WithPayloadUniqueOptimization(), WithDisableReady(), ttlOpt, timeoutOpt)
	testDelayQueue(t, q5, true)
	testQueueCommitTimeOut(t, q5)

	// q6 ---------------------------
	q6 := NewQueue(_rds, "queue-ready-unique", WithPayloadUniqueOptimization(), WithDisableDelay(), ttlOpt, timeoutOpt)
	testReadyQueue(t, q6, true)
	testQueueCommitTimeOut(t, q6)
}

func TestWithPartitionOrder(t *testing.T) {
	q := NewQueue(_rds, "test:queue", WithPartitionNum(3), WithDelMsgOnCommit(),
		WithMsgTTL(10), WithCommitTimeout(1), WithPartitionOrder())

	ctx, cancel := context.WithCancel(context.Background())

	tests := []*omq.Message{
		{Topic: "t1", Payload: omq.ByteEncoder("words1")},
		{Topic: "t1", Payload: omq.ByteEncoder("words2")},
		{Topic: "t1", Payload: omq.ByteEncoder("words3")},
		{ID: "123", Topic: "t2", Payload: omq.ByteEncoder("words4")},
		{ID: "123", Topic: "t2", Payload: omq.ByteEncoder("words5")},
		{ID: "123", Topic: "t2", Payload: omq.ByteEncoder("words6")},
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
		// t.Log(meta.partition, "---", msg.ID, "---", msg.Topic, time.Since(now).Seconds())
	}

	q.Clear(context.Background())
}

func testQueue(t *testing.T, q omq.Queue, unique bool) {
	rq := q.(*queue)

	fmt.Printf("--------------%s %s start----------------------\n", rq.partitions[0].prefix, "testQueue")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	begin := now

	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("1")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("2")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("3"), DelayAt: now.Add(1 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("4"), DelayAt: now.Add(2 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("5"), DelayAt: now.Add(3 * time.Second)}))
	count, err := q.Size(ctx)
	rNil(t, err)
	rNumber(t, count, 5)

	fetcher, err := q.Fetcher(ctx, 0)
	rNil(t, err)
	go func() {
		time.Sleep(4 * time.Second)
		cancel()
	}()

	var n int
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)
		rBytes(t, b, strconv.Itoa(n))

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}

		if n == 2 {
			break
		}
	}
	period := time.Since(now)
	if period > time.Second {
		t.Fatal("ready message delay")
	}
	fmt.Printf("period: %f s \n", period.Seconds())

	now = time.Now()
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)
		rBytes(t, b, strconv.Itoa(n))

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}
	}
	period = time.Since(now)
	if period < 3*time.Second {
		t.Fatal("delay message not delay")
	}

	rNumber(t, n, 5)

	ctx = context.Background()
	l, err := _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.ZCard(ctx, rq.partitions[0].delay).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.LLen(ctx, rq.partitions[0].ready).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	fmt.Printf("period: %f s \n", period.Seconds())
	fmt.Printf("all cost %f s\n", time.Since(begin).Seconds())
	fmt.Printf("------------------------------------\n")
}

func testDelayQueue(t *testing.T, q omq.Queue, unique bool) {
	rq := q.(*queue)

	fmt.Printf("--------------%s %s start----------------------\n", rq.partitions[0].prefix, "testDelayQueue")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	begin := now

	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("1")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("2")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("3"), DelayAt: now.Add(1 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("4"), DelayAt: now.Add(2 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("5"), DelayAt: now.Add(3 * time.Second)}))
	count, err := q.Size(ctx)
	rNil(t, err)
	rNumber(t, count, 5)

	fetcher, err := q.Fetcher(ctx, 0)
	rNil(t, err)
	go func() {
		time.Sleep(4 * time.Second)
		cancel()
	}()

	var n int
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}

		if n == 2 {
			break
		}
	}
	period := time.Since(now)
	if period > time.Second {
		t.Fatal("ready message delay")
	}
	fmt.Printf("period: %f s \n", period.Seconds())

	now = time.Now()
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}
	}
	period = time.Since(now)
	if period < 3*time.Second {
		t.Fatal("delay message not delay")
	}

	rNumber(t, n, 5)

	ctx = context.Background()
	l, err := _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.ZCard(ctx, rq.partitions[0].delay).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.LLen(ctx, rq.partitions[0].ready).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	fmt.Printf("period: %f s \n", period.Seconds())
	fmt.Printf("all cost %f s\n", time.Since(begin).Seconds())
	fmt.Printf("------------------------------------\n")
}

func testReadyQueue(t *testing.T, q omq.Queue, unique bool) {
	rq := q.(*queue)

	fmt.Printf("--------------%s %s start----------------------\n", rq.partitions[0].prefix, "testReadyQueue")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	begin := now

	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("1")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("2")}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("3"), DelayAt: now.Add(1 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("4"), DelayAt: now.Add(2 * time.Second)}))
	rNil(t, q.Produce(ctx, &omq.Message{Payload: omq.ByteEncoder("5"), DelayAt: now.Add(3 * time.Second)}))
	count, err := q.Size(ctx)
	rNil(t, err)
	rNumber(t, count, 5)

	fetcher, err := q.Fetcher(ctx, 0)
	rNil(t, err)
	go func() {
		time.Sleep(4 * time.Second)
		cancel()
	}()

	var n int
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)
		rBytes(t, b, strconv.Itoa(n))

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}

		if n == 2 {
			break
		}
	}
	period := time.Since(now)
	if period > time.Second {
		t.Fatal("ready message delay")
	}
	fmt.Printf("period: %f s \n", period.Seconds())

	now = time.Now()
	for msg := range fetcher.Messages() {
		n++
		b, err := msg.Payload.Encode()
		rNil(t, err)
		rBytes(t, b, strconv.Itoa(n))

		meta := msg.Metadata.(msgMeta)
		if unique {
			fmt.Printf("rawMsg: %s \n", msg.Metadata.(msgMeta).rawMsg)
		} else {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 1)
			fmt.Printf("msg: %s \n", b)
		}

		rNil(t, fetcher.Commit(ctx, msg))

		if !unique {
			key := rq.partitions[0].prefix + meta.msgId
			count, err := _rds.Exists(ctx, key).Result()
			rNil(t, err)
			rNumber(t, int(count), 0)
		}

		if n == 5 {
			break
		}
	}
	period = time.Since(now)
	if period > time.Second {
		t.Fatal("ready message delay")
	}

	rNumber(t, n, 5)

	l, err := _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.ZCard(ctx, rq.partitions[0].delay).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	l, err = _rds.LLen(ctx, rq.partitions[0].ready).Result()
	rNil(t, err)
	rNumber(t, int(l), 0)

	fmt.Printf("period: %f s \n", period.Seconds())
	fmt.Printf("all cost %f s\n", time.Since(begin).Seconds())
	fmt.Println("------------------------------------")
}

func testQueueCommitTimeOut(t *testing.T, q omq.Queue) {
	rq := q.(*queue)

	ctx, cancel := context.WithCancel(context.Background())
	rNil(t, q.Produce(ctx, &omq.Message{
		Topic:    "ready1",
		Payload:  omq.ByteEncoder("1"),
		MaxRetry: 2,
	}))

	fetcher, err := q.Fetcher(ctx, 2)
	rNil(t, err)

	go func() {
		time.Sleep(11 * time.Second)
		cancel()
	}()

	now := time.Now()
	begin := now
	var n int
	for _ = range fetcher.Messages() {
		n++
		if n == 3 {
			fmt.Printf("retry 3 times cost %f s\n", time.Since(now).Seconds())
			break
		}
	}

	count, err := _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(count), 0)

	now = time.Now()
	rNil(t, q.Produce(ctx, &omq.Message{
		Topic:    "delay1",
		Payload:  omq.ByteEncoder("2"),
		MaxRetry: -1,
		DelayAt:  now.Add(time.Second),
	}))
	for _ = range fetcher.Messages() {
		n++
		if n == 5 {
			fmt.Printf("retry default 2 times cost %f s\n", time.Since(now).Seconds())
			break
		}
	}

	count, err = _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(count), 0)

	now = time.Now()
	rNil(t, q.Produce(ctx, &omq.Message{
		Topic:   "delay2",
		Payload: omq.ByteEncoder("2"),
		DelayAt: time.Now().Add(time.Second),
	}))

	for _ = range fetcher.Messages() {
		n++
		if n == 6 {
			break
		}
	}

	count, err = _rds.ZCard(ctx, rq.partitions[0].unCommit).Result()
	rNil(t, err)
	rNumber(t, int(count), 0)

	count, err = _rds.ZCard(ctx, rq.partitions[0].delay).Result()
	rNil(t, err)
	rNumber(t, int(count), 0)

	count, err = _rds.LLen(ctx, rq.partitions[0].ready).Result()
	rNil(t, err)
	rNumber(t, int(count), 0)

	fmt.Printf("all cost %f s\n", time.Since(begin).Seconds())
	fmt.Printf("--------------%s %s complete----------------------\n", rq.partitions[0].prefix, "testQueueCommitTimeOut")
}

func rNil(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Fatalf("unexpected error: %v", err)
	}
}

func rNumber(t *testing.T, got, want int) {
	if got != want {
		t.Helper()
		t.Fatalf("got: %d, want: %d", got, want)
	}
}

func rBytes(t *testing.T, got []byte, want string) {
	if string(got) != want {
		t.Helper()
		t.Fatalf("got: %s, want: %s", got, want)
	}
}

func TestQueueLock(t *testing.T) {
	var w sync.WaitGroup
	w.Add(2)

	go lockLoop(&w, 1)
	go lockLoop(&w, 2)

	w.Wait()
}

func lockLoop(w *sync.WaitGroup, id int) {
	var (
		locked       bool
		lockInterval time.Duration
		lockTtl      = time.Duration(6)
		lockTime     time.Time
	)

	ctx := context.Background()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var n int
	for {
		if lockInterval == 0 {
			locked = _rds.SetNX(ctx, "lock", "1", lockTtl*time.Second).Val()
			if locked {
				now := time.Now()
				if !lockTime.IsZero() {
					fmt.Printf("locked interval %f s; id: %d\n", now.Sub(lockTime).Seconds(), id)
				} else {
					fmt.Printf("locked id: %d\n", id)
				}
				lockTime = now
			}
		}

		lockInterval++
		if locked {
			time.Sleep(10 * time.Millisecond)

			if lockInterval > lockTtl { // maybe no server get lock on period of lockTtl
				lockInterval = 0
			}
		} else {
			if lockInterval >= 3 {
				lockInterval = 0
			}
		}

		n++
		if n == 60 {
			break
		}

		<-ticker.C

	}

	w.Done()
}

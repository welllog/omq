package redisq

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/welllog/omq"
)

func TestQueueProduce(t *testing.T) {
    q := NewQueue(_rds, "test:queue", WithPartitionNum(2), WithDelMsgOnCommit(), WithMsgTTL(10))

    ctx, cancel := context.WithCancel(context.Background())

    now := time.Now()
    msgs := []*omq.Message{
        {
            Topic:   "ready1",
            Payload: omq.ByteEncoder("words1"),
        },
        {
            Topic:   "ready2",
            Payload: omq.ByteEncoder("words2"),
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
        time.Sleep(1100 * time.Millisecond)
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
        WithMsgTTL(10), WithSafeMode(), WithCommitTimeout(1))

    ctx, cancel := context.WithCancel(context.Background())

    q.Produce(ctx, &omq.Message{
        Topic:   "ready1",
        Payload: omq.ByteEncoder("words1"),
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

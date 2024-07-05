package omq

import (
	"context"
	"time"
)

type Message struct {
	ID      string
	Topic   string
	Payload Encoder
	DelayAt time.Time
	// MaxRetry is the max retry times, if it is 0, it means no retry
	// if it is -1, it use the default max retry times
	MaxRetry int
	Metadata any
}

type Fetcher interface {
	Messages() <-chan *Message
	Commit(ctx context.Context, msg *Message) error
}

type Queue interface {
	Size(ctx context.Context) (int, error)
	Produce(ctx context.Context, msg *Message) error
	Fetcher(ctx context.Context, bufferSize int) (Fetcher, error)
	Clear(ctx context.Context) error
}

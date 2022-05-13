package redisq

import (
	"context"

	"github.com/welllog/omq"
)

type commitFunc func(ctx context.Context, msg *omq.Message) error

type fetcher struct {
	ch     chan *omq.Message
	handle commitFunc
}

func newFetcher(bufferSize int, handle commitFunc) *fetcher {
	return &fetcher{
		ch:     make(chan *omq.Message, bufferSize),
		handle: handle,
	}
}

func (f *fetcher) Messages() <-chan *omq.Message {
	return f.ch
}

func (f *fetcher) Commit(ctx context.Context, msg *omq.Message) error {
	return f.handle(ctx, msg)
}

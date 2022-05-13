package redisq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/welllog/omq"
)

type queue struct {
	rds            redis.UniversalClient
	keyPrefix      string
	disableDelay   bool
	msgTTL         int64
	logger         omq.Logger
	partitionNum   int
	safeMode       bool
	delMsgOnCommit bool
	useBlockPop    bool
	blockTimeout   int
	sleepOnEmpty   time.Duration
	commitTimeout  int
	partitions     []partition
	counter        uint32
	lockKey        string
}

func NewQueue(rds redis.UniversalClient, keyPrefix string, opts ...Option) omq.Queue {
	o := defOptions
	for _, opt := range opts {
		opt(&o)
	}

	q := &queue{
		rds:            rds,
		keyPrefix:      keyPrefix,
		disableDelay:   o.disableDelay,
		msgTTL:         o.msgTTL,
		logger:         o.logger,
		partitionNum:   o.partitionNum,
		safeMode:       o.safeMode,
		delMsgOnCommit: o.delMsgOnCommit,
		useBlockPop:    o.useBlockPop,
		blockTimeout:   o.blockTimeout,
		sleepOnEmpty:   o.sleepOnEmpty,
		commitTimeout:  o.commitTimeout,
		partitions:     make([]partition, o.partitionNum),
		counter:        0,
		lockKey:        keyPrefix + ":lock",
	}

	for i := 0; i < o.partitionNum; i++ {
		q.partitions[i] = newPartition(i, keyPrefix, rds)
	}
	return q
}

func (q *queue) Size(ctx context.Context) (int, error) {
	var totalSize int
	for i := range q.partitions {
		size, err := q.partitions[i].size(ctx)
		if err != nil {
			return 0, err
		}
		totalSize += size
	}
	return totalSize, nil
}

func (q *queue) Produce(ctx context.Context, msg *omq.Message) error {
	msg.ID = omq.UUID()
	var index int
	if q.partitionNum > 1 {
		count := atomic.AddUint32(&q.counter, 1)
		index = int(count-1) % q.partitionNum
	}

	now := time.Now()
	delay := msg.DelayAt.Unix() - now.Unix()
	if q.disableDelay || msg.DelayAt.IsZero() || delay <= 0 {
		msg.DelayAt = now
		return q.partitions[index].pushReady(ctx, msg, q.msgTTL)
	}

	return q.partitions[index].pushDelay(ctx, msg, delay+q.msgTTL)
}

func (q *queue) Clear(ctx context.Context) error {
	for i := range q.partitions {
		if err := q.partitions[i].clear(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (q *queue) Fetcher(ctx context.Context, bufferSize int) (omq.Fetcher, error) {
	f := q.initFetcher(bufferSize)

	if !q.disableDelay {
		go func() {
			q.delayToReady(ctx)
		}()
	}

	go func() {
		q.writeMessage(ctx, f.ch)
	}()
	return f, nil
}

func (q *queue) delayToReady(ctx context.Context) {
	var (
		locked        bool
		lockInterval  time.Duration
		checkInterval int
		unix          int64
		lockTtl       = time.Duration(5)
		commitTimeout = q.commitTimeout
	)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	now := time.Now()
	for {
		unix = now.Unix()
		select {
		case <-ctx.Done():
			return
		default:
			if lockInterval == 0 {
				locked = q.tryLock(ctx, lockTtl*time.Second)
			}

			if locked {
				for i := range q.partitions {
					if err := q.partitions[i].delayToReady(ctx, unix); err != nil && !errors.Is(err, context.Canceled) {
						q.logger.Warnf("delayToReady error: %s", err)
					}
				}

				if q.safeMode && checkInterval == 0 {
					for i := range q.partitions {
						if err := q.partitions[i].commitTimeoutToReady(ctx, unix-int64(commitTimeout)); err != nil && !errors.Is(err, context.Canceled) {
							q.logger.Warnf("commitTimeoutToReady error: %s", err)
						}
					}
				}
			}

			lockInterval++
			if lockInterval >= lockTtl {
				lockInterval = 0
			}

			checkInterval++
			if checkInterval >= commitTimeout {
				checkInterval = 0
			}

			now = <-ticker.C
		}
	}
}

func (q *queue) writeMessage(ctx context.Context, ch chan<- *omq.Message) {
	var popMessage func(p *partition, ch chan<- *omq.Message) error
	if q.safeMode {
		popMessage = func(p *partition, ch chan<- *omq.Message) error {
			msgs, err := p.safePopReady(context.TODO(), 5)
			if err != nil {
				return err
			}
			for i := range msgs {
				ch <- msgs[i]
			}

			if len(msgs) < 5 {
				time.Sleep(q.sleepOnEmpty)
			}
			return nil
		}
	} else if q.useBlockPop {
		timeout := time.Duration(q.blockTimeout) * time.Second
		popMessage = func(p *partition, ch chan<- *omq.Message) error {
			msgs, err := p.bPopReady(context.TODO(), timeout)
			if err != nil {
				return err
			}

			for i := range msgs {
				ch <- msgs[i]
			}
			return nil
		}
	} else {
		popMessage = func(p *partition, ch chan<- *omq.Message) error {
			msgs, err := p.popReady(context.TODO(), 5)
			if err != nil {
				return err
			}

			for i := range msgs {
				ch <- msgs[i]
			}

			if len(msgs) < 5 {
				time.Sleep(q.sleepOnEmpty)
			}
			return nil
		}
	}

	var w sync.WaitGroup

	for i := range q.partitions {
		w.Add(1)
		go func(p *partition) {
			defer w.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := popMessage(p, ch); err != nil && !errors.Is(err, context.Canceled) {
						q.logger.Warnf("popMessage error: %s", err)
					}
				}
			}
		}(&q.partitions[i])
	}

	w.Wait()
	close(ch)
}

func (q *queue) tryLock(ctx context.Context, ttl time.Duration) bool {
	return q.rds.SetNX(ctx, q.lockKey, 1, ttl).Val()
}

func (q *queue) delMsg(ctx context.Context, msg *omq.Message) error {
	index, ok := msg.Metadata.(int)
	if ok {
		return q.partitions[index].delMsg(ctx, msg.ID)
	}
	return nil
}

func (q *queue) commitMsg(ctx context.Context, msg *omq.Message) error {
	index, ok := msg.Metadata.(int)
	if ok {
		return q.partitions[index].commitMsg(ctx, msg.ID)
	}
	return nil
}

func (q *queue) commitAndDelMsg(ctx context.Context, msg *omq.Message) error {
	index, ok := msg.Metadata.(int)
	if ok {
		return q.partitions[index].commitAndDelMsg(ctx, msg.ID)
	}
	return nil
}

func (q *queue) initFetcher(bufferSize int) *fetcher {
	if q.safeMode {
		if q.delMsgOnCommit {
			return newFetcher(bufferSize, q.commitAndDelMsg)
		}
		return newFetcher(bufferSize, q.commitMsg)
	}

	if q.delMsgOnCommit {
		return newFetcher(bufferSize, q.delMsg)
	}

	return newFetcher(bufferSize, func(ctx context.Context, msg *omq.Message) error {
		return nil
	})
}

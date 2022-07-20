package redisq

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	delMsgOnCommit bool
	partitionOrder bool
	sleepOnEmpty   time.Duration
	commitTimeout  int
	maxRetry       int
	partitions     []partition
	counter        uint32
	lockKey        string
}

type msgMeta struct {
	partition int
	msgId     string
	retry     int
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
		delMsgOnCommit: o.delMsgOnCommit,
		partitionOrder: o.partitionOrder,
		sleepOnEmpty:   o.sleepOnEmpty,
		commitTimeout:  o.commitTimeout,
		maxRetry:       o.maxRetry,
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
	msgID := omq.UUID()

	var index int
	if q.partitionNum > 1 {
		if q.partitionOrder {
			index = _BKDRHash(_StringToBytes(msg.ID+msg.Topic)) % q.partitionNum
		} else {
			count := atomic.AddUint32(&q.counter, 1)
			index = int(count-1) % q.partitionNum
		}
	}

	if msg.MaxRetry <= 0 {
		msg.MaxRetry = q.maxRetry
	}
	now := time.Now()
	delay := msg.DelayAt.Unix() - now.Unix()
	if q.disableDelay || msg.DelayAt.IsZero() || delay <= 0 {
		msg.DelayAt = now
		return q.partitions[index].pushReady(ctx, msgID, msg, q.msgTTL)
	}

	return q.partitions[index].pushDelay(ctx, msgID, msg, delay+q.msgTTL)
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

	go func() {
		q.toReady(ctx)
	}()

	go func() {
		q.writeMessage(ctx, f.ch)
	}()
	return f, nil
}

func (q *queue) toReady(ctx context.Context) {
	var (
		locked        bool
		lockInterval  time.Duration
		unix          int64
		lockTtl       = time.Duration(5)
		commitTimeout = q.commitTimeout
		taskNum       = 6
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
				if !q.disableDelay {
					for i := range q.partitions {
						for {
							num, err := q.partitions[i].delayToReady(ctx, unix, taskNum)
							if num == taskNum {
								continue
							}
							if err != nil && !errors.Is(err, context.Canceled) {
								q.logger.Warnf("delayToReady error: %s", err)
							}
							break
						}
					}
				}

				for i := range q.partitions {
					for {
						num, err := q.partitions[i].commitTimeoutToReady(ctx, unix-int64(commitTimeout), taskNum)
						if num == taskNum {
							continue
						}
						if err != nil && !errors.Is(err, context.Canceled) {
							q.logger.Warnf("commitTimeoutToReady error: %s", err)
						}
						break
					}
				}
			}

			lockInterval++
			if lockInterval >= lockTtl { // maybe no server get lock on period of lockTtl
				lockInterval = 0
			}

			now = <-ticker.C
		}
	}
}

func (q *queue) writeMessage(ctx context.Context, ch chan<- *omq.Message) {
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
					msg, err := p.fetchReady(ctx)
					if err == nil {
						ch <- msg
					} else if errors.Is(err, errNoMessage) {
						time.Sleep(q.sleepOnEmpty)
					} else if !errors.Is(err, context.Canceled) {
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

func (q *queue) commitMsg(ctx context.Context, msg *omq.Message) error {
	meta, ok := msg.Metadata.(msgMeta)
	if ok && meta.retry > 0 {
		return q.partitions[meta.partition].commitMsg(ctx, meta.msgId)
	}
	return nil
}

func (q *queue) commitAndDelMsg(ctx context.Context, msg *omq.Message) error {
	meta, ok := msg.Metadata.(msgMeta)
	if ok {
		if meta.retry > 0 {
			return q.partitions[meta.partition].commitAndDelMsg(ctx, meta.msgId)
		}
		return q.partitions[meta.partition].delMsg(ctx, meta.msgId)
	}
	return nil
}

func (q *queue) initFetcher(bufferSize int) *fetcher {
	if q.delMsgOnCommit {
		return newFetcher(bufferSize, q.commitAndDelMsg)
	}
	return newFetcher(bufferSize, q.commitMsg)
}

func _StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

func _BKDRHash(str []byte) int {
	var seed uint64 = 131 // 31 131 1313 13131 131313 etc..
	var hash uint64 = 0
	for i := 0; i < len(str); i++ {
		hash = hash*seed + uint64(str[i])
	}
	return int(hash & uint64(math.MaxInt)) // 0x7FFFFFFFFFFFFFFF = 2^63 - 1
}

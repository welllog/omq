package redisq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/welllog/omq"
)

type queue struct {
	rds                       redis.UniversalClient
	disableDelay              bool
	disableReady              bool
	delMsgOnCommit            bool
	partitionOrder            bool
	payloadUniqueOptimization bool
	msgTTL                    int64
	logger                    omq.Logger
	partitionNum              int
	sleepOnEmpty              time.Duration
	commitTimeout             int64
	maxRetry                  int
	partitions                []partition
	counter                   uint32
	lockKey                   string
	encodeFunc                func(*omq.Message) (string, error)
	decodeFunc                func(string, *omq.Message) error
	sizeFunc                  func(context.Context, partition) (int, error)
	pushFunc                  func(context.Context, partition, *omq.Message, string, int64) error
	commitTimeoutFunc         func(context.Context, partition, int64, int64) (int, error)
	fetchFunc                 func(context.Context, partition, func(string, *omq.Message) error) (*omq.Message, error)
}

type msgMeta struct {
	partition int
	msgId     string
	retry     int
	rawMsg    string
}

func NewQueue(rds redis.UniversalClient, keyPrefix string, opts ...Option) omq.Queue {
	o := defOptions
	for _, opt := range opts {
		opt(&o)
	}

	var (
		sizeFunc          func(context.Context, partition) (int, error)
		pushFunc          func(context.Context, partition, *omq.Message, string, int64) error
		commitTimeoutFunc func(context.Context, partition, int64, int64) (int, error)
		fetchFunc         func(context.Context, partition, func(string, *omq.Message) error) (*omq.Message, error)
	)
	if o.disableDelay { // use ready
		o.disableReady = false
		sizeFunc = readySize

		if o.payloadUniqueOptimization {
			pushFunc = uniquePushReady
			commitTimeoutFunc = uniqueCommitTimeoutToReady
			fetchFunc = uniqueFetchReadyMessage
		} else {
			pushFunc = pushReady
			commitTimeoutFunc = commitTimeoutToReady
			fetchFunc = fetchReadyMessage
		}
	} else if o.disableReady { // use delay
		o.disableDelay = false
		sizeFunc = delaySize

		if o.payloadUniqueOptimization {
			pushFunc = uniquePushDelay
			commitTimeoutFunc = uniqueCommitTimeoutToDelay
			fetchFunc = uniqueFetchDelayMessage
		} else {
			pushFunc = pushDelay
			commitTimeoutFunc = commitTimeoutToDelay
			fetchFunc = fetchDelayMessage
		}
	} else { // use ready and delay
		sizeFunc = size

		if o.payloadUniqueOptimization {
			pushFunc = uniquePush
			commitTimeoutFunc = uniqueCommitTimeoutToReady
			fetchFunc = uniqueFetchReadyMessage
		} else {
			pushFunc = push
			commitTimeoutFunc = commitTimeoutToReady
			fetchFunc = fetchReadyMessage
		}
	}

	q := &queue{
		rds:                       rds,
		disableDelay:              o.disableDelay,
		disableReady:              o.disableReady,
		delMsgOnCommit:            o.delMsgOnCommit,
		partitionOrder:            o.partitionOrder,
		payloadUniqueOptimization: o.payloadUniqueOptimization,
		msgTTL:                    o.msgTTL,
		logger:                    o.logger,
		partitionNum:              o.partitionNum,
		sleepOnEmpty:              o.sleepOnEmpty,
		commitTimeout:             o.commitTimeout,
		maxRetry:                  o.maxRetry,
		partitions:                make([]partition, o.partitionNum),
		counter:                   0,
		lockKey:                   keyPrefix + ":lock",
		encodeFunc:                o.encodeFunc,
		decodeFunc:                o.decodeFunc,
		sizeFunc:                  sizeFunc,
		pushFunc:                  pushFunc,
		commitTimeoutFunc:         commitTimeoutFunc,
		fetchFunc:                 fetchFunc,
	}

	for i := 0; i < o.partitionNum; i++ {
		q.partitions[i] = newPartition(i, keyPrefix, rds)
	}
	return q
}

func (q *queue) Size(ctx context.Context) (int, error) {
	var totalSize int
	for i := range q.partitions {
		n, err := q.sizeFunc(ctx, q.partitions[i])
		if err != nil {
			return 0, err
		}
		totalSize += n
	}
	return totalSize, nil
}

func (q *queue) Produce(ctx context.Context, msg *omq.Message) error {
	var index int
	if q.partitionNum > 1 {
		if q.partitionOrder {
			index = _BKDRHash(msg.ID) % q.partitionNum
		} else {
			count := atomic.AddUint32(&q.counter, 1)
			index = int(count-1) % q.partitionNum
		}
	}

	if msg.MaxRetry < 0 {
		msg.MaxRetry = q.maxRetry
	}

	rawMsg, err := q.encodeFunc(msg)
	if err != nil {
		return fmt.Errorf("encode msg failed: %w", err)
	}

	ttl := q.msgTTL
	if q.disableReady {
		now := time.Now()
		delay := msg.DelayAt.Sub(now)
		if delay > 0 {
			ttl += int64(delay.Seconds())
		} else {
			msg.DelayAt = now
		}
	}
	return q.pushFunc(ctx, q.partitions[index], msg, rawMsg, ttl)
}

func (q *queue) Clear(ctx context.Context) error {
	for i := range q.partitions {
		if err := clean(ctx, q.partitions[i]); err != nil {
			return err
		}
	}
	return nil
}

func (q *queue) Fetcher(ctx context.Context, bufferSize int) (omq.Fetcher, error) {
	f := q.initFetcher(bufferSize)

	go func() {
		q.mutexLoop(ctx)
	}()

	go func() {
		q.fetchMessage(ctx, f.ch)
	}()

	return f, nil
}

func (q *queue) mutexLoop(ctx context.Context) {
	var (
		locked       bool
		lockInterval time.Duration
		unix         int64
		lockTtl      = time.Duration(6)
		batchSize    = int64(6)
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
			lockInterval++

			if locked {
				if !q.disableReady && !q.disableDelay {
					q.delayToReadyTask(ctx, unix, batchSize)
				}

				q.commitTimeoutTask(ctx, unix, batchSize)

				if lockInterval > lockTtl { // maybe no server get lock on period of lockTtl
					lockInterval = 0
				}
			} else {
				if lockInterval >= lockTtl/2 {
					lockInterval = 0
				}
			}

			now = <-ticker.C
		}
	}
}

func (q *queue) fetchMessage(ctx context.Context, ch chan<- *omq.Message) {
	workGroup := make([]int, len(q.partitions))
	for i := range q.partitions {
		workGroup[i] = q.partitions[i].id
	}

	sleepGroup := &sleepQueue{}
	sleepGroup.init(len(q.partitions))
	sleepPool := sleepPtnPool{}

	for {
		remain := len(workGroup) - 1
		for i := remain; i >= 0; i-- {
			msg, err := q.fetchFunc(ctx, q.partitions[workGroup[i]], q.decodeFunc)
			if err == nil {
				ch <- msg
			} else if errors.Is(err, errNoMessage) {
				workGroup[i], workGroup[remain] = workGroup[remain], workGroup[i]
				remain--
			} else if errors.Is(err, context.Canceled) {
				close(ch)
				return
			} else {
				q.logger.Warnf("fetch message error: %s", err)
			}
		}

		workGroupDrop := workGroup[remain+1:]
		workGroup = workGroup[:remain+1]
		if len(workGroupDrop) > 0 {
			sleep := sleepPool.GetBy(workGroupDrop)

			if sleep.timer == nil {
				sleep.timer = time.NewTimer(q.sleepOnEmpty)
			} else {
				sleep.timer.Reset(q.sleepOnEmpty)
			}
			sleepGroup.Push(sleep)

			if len(workGroup) == 0 {
				g := sleepGroup.Pop()
				select {
				case <-g.timer.C:
					workGroup = append(workGroup, g.ptn...)
					sleepPool.Put(g)
					continue
				case <-ctx.Done():
					close(ch)
					return
				}
			}
		}

	wait:
		for {
			g := sleepGroup.Peek()
			if g == nil {
				break
			}

			select {
			case <-g.timer.C:
				workGroup = append(workGroup, g.ptn...)
				sleepGroup.Pop()
				sleepPool.Put(g)

			default:
				break wait
			}
		}

	}

}

func (q *queue) delayToReadyTask(ctx context.Context, now, batchSize int64) {
stop:
	for i := range q.partitions {
		for {
			num, err := delayToReady(ctx, q.partitions[i], now, batchSize)
			if int64(num) == batchSize {
				continue
			}

			if err != nil {
				if errors.Is(err, context.Canceled) {
					break stop
				}

				q.logger.Warnf("delayToReady error: %s", err)
			}

			break
		}
	}
}

func (q *queue) commitTimeoutTask(ctx context.Context, now, batchSize int64) {
stop:
	for i := range q.partitions {
		for {
			num, err := q.commitTimeoutFunc(ctx, q.partitions[i], now-q.commitTimeout, batchSize)
			if int64(num) == batchSize {
				continue
			}

			if err != nil {
				if errors.Is(err, context.Canceled) {
					break stop
				}

				q.logger.Warnf("commitTimeoutToReady error: %s", err)
			}

			break
		}
	}
}

func (q *queue) tryLock(ctx context.Context, ttl time.Duration) bool {
	return q.rds.SetNX(ctx, q.lockKey, 1, ttl).Val()
}

func (q *queue) commitMsg(ctx context.Context, msg *omq.Message) error {
	meta, ok := msg.Metadata.(msgMeta)
	if !ok {
		return errMetadata
	}

	if meta.retry <= 0 {
		return nil
	}

	if q.payloadUniqueOptimization {
		return uniqueCommitMsg(ctx, q.partitions[meta.partition], meta)
	}

	return commitMsg(ctx, q.partitions[meta.partition], meta)
}

func (q *queue) commitAndDelMsg(ctx context.Context, msg *omq.Message) error {
	meta, ok := msg.Metadata.(msgMeta)
	if !ok {
		return errMetadata
	}

	if q.payloadUniqueOptimization {
		return uniqueCommitMsg(ctx, q.partitions[meta.partition], meta)
	}

	return commitAndDelMsg(ctx, q.partitions[meta.partition], meta)
}

func (q *queue) initFetcher(bufferSize int) *fetcher {
	if q.delMsgOnCommit {
		return newFetcher(bufferSize, q.commitAndDelMsg)
	}
	return newFetcher(bufferSize, q.commitMsg)
}

func _BKDRHash(s string) int {
	var seed uint64 = 131 // 31 131 1313 13131 131313 etc..
	var hash uint64 = 0
	for i := 0; i < len(s); i++ {
		hash = hash*seed + uint64(s[i])
	}
	return int(hash & math.MaxInt) // 0x7FFFFFFFFFFFFFFF = 2^63 - 1
}

package redisq

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/welllog/omq"
)

func clean(ctx context.Context, p partition) error {
	return p.rds.Del(ctx, p.ready, p.delay, p.unCommit).Err()
}

func size(ctx context.Context, p partition) (int, error) {
	return _sizeCmd.Run(ctx, p.rds, []string{p.ready, p.delay, p.unCommit}).Int()
}

func delaySize(ctx context.Context, p partition) (int, error) {
	return _delaySizeCmd.Run(ctx, p.rds, []string{p.delay, p.unCommit}).Int()
}

func readySize(ctx context.Context, p partition) (int, error) {
	return _readySizeCmd.Run(ctx, p.rds, []string{p.ready, p.unCommit}).Int()
}

func push(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	delay := time.Until(msg.DelayAt)
	if msg.DelayAt.IsZero() || delay <= 0 {
		return pushReady(ctx, p, msg, rawMsg, ttl)
	}
	return pushDelay(ctx, p, msg, rawMsg, ttl+int64(delay.Seconds()))
}

func uniquePush(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	if msg.DelayAt.IsZero() || time.Since(msg.DelayAt) >= 0 {
		return uniquePushReady(ctx, p, msg, rawMsg, ttl)
	}
	return uniquePushDelay(ctx, p, msg, rawMsg, ttl)
}

func pushReady(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	msgID := omq.UUID()
	return _pushReadyCmd.Run(ctx, p.rds, []string{p.ready, p.prefix + msgID},
		msgID, rawMsg, msg.MaxRetry, ttl).Err()
}

func pushDelay(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	msgID := omq.UUID()
	return _pushDelayCmd.Run(ctx, p.rds, []string{p.delay, p.prefix + msgID},
		msg.DelayAt.Unix(), msgID, rawMsg, msg.MaxRetry, ttl).Err()
}

func uniquePushReady(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	return p.rds.RPush(ctx, p.ready, rawMsg).Err()
}

func uniquePushDelay(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	return p.rds.ZAdd(ctx, p.delay, redis.Z{Score: float64(msg.DelayAt.Unix()), Member: rawMsg}).Err()
}

func delayToReady(ctx context.Context, p partition, delayAt, num int64) (int, error) {
	return _delayToReadyCmd.Run(ctx, p.rds, []string{p.delay, p.ready}, delayAt, num).Int()
}

func commitTimeoutToReady(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	return _unCommitToReadyCmd.Run(ctx, p.rds, []string{p.unCommit, p.ready}, timeoutAt, num, p.prefix).Int()
}

func commitTimeoutToDelay(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	retryAt := time.Now().Unix() + 2
	return _unCommitToDelayCmd.Run(ctx, p.rds, []string{p.unCommit, p.delay}, timeoutAt, num, p.prefix, retryAt).Int()
}

func uniqueCommitTimeoutToReady(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	return _uniqueUnCommitToReadyCmd.Run(ctx, p.rds, []string{p.unCommit, p.ready}, timeoutAt, num).Int()
}

func uniqueCommitTimeoutToDelay(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	retryAt := time.Now().Unix() + 2
	return _uniqueUnCommitToDelayCmd.Run(ctx, p.rds, []string{p.unCommit, p.delay}, timeoutAt, num, retryAt).Int()
}

func fetchReadyMessage(ctx context.Context, p partition, decodeFunc func(*omq.Message) (string, error)) (*omq.Message, error) {
	arr, err := _fetchReadyCmd.Run(ctx, p.rds, []string{p.ready, p.unCommit}, p.prefix, time.Now().Unix()).StringSlice()
	if err != nil {
		return "", "", 0, err
	}

	if len(arr) != 3 {
		return "", "", 0, errNoMessage
	}

	retry, _ = strconv.Atoi(arr[2])
	return arr[0], arr[1], retry, nil
}

func uniqueFetchReadyMessage(ctx context.Context, p partition) (msgId, rawMsg string, retry int, err error) {
	rawMsg, err = p.rds.LPop(ctx, p.ready).Result()
	if err != nil {
		return "", "", 0, err
	}
	return "", rawMsg, 0, nil
}

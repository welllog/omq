package redisq

import (
	"context"
	"strconv"
	"strings"
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
	rawMsg = "@" + strconv.Itoa(msg.MaxRetry) + "#" + rawMsg
	return p.rds.RPush(ctx, p.ready, rawMsg).Err()
}

func uniquePushDelay(ctx context.Context, p partition, msg *omq.Message, rawMsg string, ttl int64) error {
	rawMsg = "@" + strconv.Itoa(msg.MaxRetry) + "#" + rawMsg
	return p.rds.ZAdd(ctx, p.delay, redis.Z{Score: float64(msg.DelayAt.Unix()), Member: rawMsg}).Err()
}

func delayToReady(ctx context.Context, p partition, delayAt, num int64) (int, error) {
	return _delayToReadyCmd.Run(ctx, p.rds, []string{p.delay, p.ready}, delayAt, num).Int()
}

func commitTimeoutToReady(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	return _unCommitToReadyCmd.Run(ctx, p.rds, []string{p.unCommit, p.ready}, timeoutAt, num, p.prefix).Int()
}

func commitTimeoutToDelay(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	retryAt := time.Now().Unix()
	return _unCommitToDelayCmd.Run(ctx, p.rds, []string{p.unCommit, p.delay}, timeoutAt, num, p.prefix, retryAt).Int()
}

func uniqueCommitTimeoutToReady(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	return _uniqueUnCommitToReadyCmd.Run(ctx, p.rds, []string{p.unCommit, p.ready}, timeoutAt, num).Int()
}

func uniqueCommitTimeoutToDelay(ctx context.Context, p partition, timeoutAt, num int64) (int, error) {
	retryAt := time.Now().Unix()
	return _uniqueUnCommitToDelayCmd.Run(ctx, p.rds, []string{p.unCommit, p.delay}, timeoutAt, num, retryAt).Int()
}

func fetchReadyMessage(ctx context.Context, p partition, decodeFunc func(string, *omq.Message) error) (*omq.Message, error) {
	arr, err := _fetchReadyCmd.Run(ctx, p.rds, []string{p.ready, p.unCommit}, p.prefix, time.Now().Unix()).StringSlice()
	if err != nil {
		return nil, err
	}

	if len(arr) != 3 {
		return nil, errNoMessage
	}

	var msg omq.Message
	err = decodeFunc(arr[1], &msg)
	if err != nil {
		return nil, err
	}

	retry, _ := strconv.Atoi(arr[2])
	msg.Metadata = msgMeta{
		partition: p.id,
		msgId:     arr[0],
		retry:     retry,
	}

	return &msg, nil
}

func uniqueFetchReadyMessage(ctx context.Context, p partition, decodeFunc func(string, *omq.Message) error) (*omq.Message, error) {
	arr, err := _uniqueFetchReadyCmd.Run(ctx, p.rds, []string{p.ready, p.unCommit}, time.Now().Unix()).StringSlice()
	if err != nil {
		return nil, err
	}

	if len(arr) != 2 {
		return nil, errNoMessage
	}

	index := strings.Index(arr[0], "#")
	var msg omq.Message
	err = decodeFunc(arr[0][index+1:], &msg)
	if err != nil {
		return nil, err
	}

	retry, _ := strconv.Atoi(arr[1])
	msg.Metadata = msgMeta{
		partition: p.id,
		retry:     retry,
		rawMsg:    arr[0][index+1:],
	}
	return &msg, nil
}

func fetchDelayMessage(ctx context.Context, p partition, decodeFunc func(string, *omq.Message) error) (*omq.Message, error) {
	arr, err := _fetchDelayCmd.Run(ctx, p.rds, []string{p.delay, p.unCommit}, p.prefix, time.Now().Unix()).StringSlice()
	if err != nil {
		return nil, err
	}

	if len(arr) != 3 {
		return nil, errNoMessage
	}

	var msg omq.Message
	err = decodeFunc(arr[1], &msg)
	if err != nil {
		return nil, err
	}

	retry, _ := strconv.Atoi(arr[2])
	msg.Metadata = msgMeta{
		partition: p.id,
		msgId:     arr[0],
		retry:     retry,
	}

	return &msg, nil
}

func uniqueFetchDelayMessage(ctx context.Context, p partition, decodeFunc func(string, *omq.Message) error) (*omq.Message, error) {
	arr, err := _uniqueFetchDelayCmd.Run(ctx, p.rds, []string{p.delay, p.unCommit}, time.Now().Unix()).StringSlice()
	if err != nil {
		return nil, err
	}

	if len(arr) != 2 {
		return nil, errNoMessage
	}

	index := strings.Index(arr[0], "#")
	var msg omq.Message
	err = decodeFunc(arr[0][index+1:], &msg)
	if err != nil {
		return nil, err
	}

	retry, _ := strconv.Atoi(arr[1])
	msg.Metadata = msgMeta{
		partition: p.id,
		retry:     retry,
		rawMsg:    arr[0][index+1:],
	}
	return &msg, nil
}

func commitMsg(ctx context.Context, p partition, meta msgMeta) error {
	return p.rds.ZRem(ctx, p.unCommit, meta.msgId).Err()
}

func uniqueCommitMsg(ctx context.Context, p partition, meta msgMeta) error {
	rawMsg := "@" + strconv.Itoa(meta.retry-1) + "#" + meta.rawMsg
	return p.rds.ZRem(ctx, p.unCommit, rawMsg).Err()
}

func commitAndDelMsg(ctx context.Context, p partition, meta msgMeta) error {
	if meta.retry > 0 {
		return _removeMsgCmd.Run(ctx, p.rds, []string{p.unCommit, p.prefix + meta.msgId}, meta.msgId).Err()
	}

	return p.rds.Del(ctx, p.prefix+meta.msgId).Err()
}

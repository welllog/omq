package redisq

import (
	"time"

	"github.com/welllog/omq"
)

type options struct {
	// Whether to disable the delayed queue or not.
	// turning it off when not using the delayed queue can reduce the overhead
	disableDelay bool

	// Whether to disable the ready queue or not.
	// turning it off when not using the ready queue can reduce the overhead
	disableReady bool

	// Whether to actively delete messages when submitted, only for non-safe mode
	delMsgOnCommit bool

	// Messages are kept in order within a partition, turning on this option will route messages by omq.Message.ID
	partitionOrder bool

	// when the message payload is unique string, and turn on this option, the message will be stored in a zset
	payloadUniqueOptimization bool

	// Message expiration time, in seconds
	msgTTL int64

	logger omq.Logger

	// The number of partitions.
	// which are randomly distributed to nodes when using a redis cluster
	partitionNum int

	// sleep time when there is no data in non-blocking
	sleepOnEmpty time.Duration

	// How long does it take for a message to rejoin the queue when it has not been submitted in safe mode, in seconds
	commitTimeout int64

	// Max retry times when commit timeout
	maxRetry int

	encodeFunc func(*omq.Message) (string, error)

	decodeFunc func(string, *omq.Message) error
}

var defOptions = options{
	disableDelay:              false,
	disableReady:              false,
	delMsgOnCommit:            false,
	partitionOrder:            false,
	payloadUniqueOptimization: false,
	msgTTL:                    86400,
	logger:                    omq.DefLogger{},
	partitionNum:              1,
	sleepOnEmpty:              300 * time.Millisecond,
	commitTimeout:             20,
	maxRetry:                  1,
	encodeFunc:                encodeMsg,
	decodeFunc:                decodeMsg,
}

type MessageCodec interface {
	Encode(*omq.Message) (string, error)
	Decode(string, *omq.Message) error
}

type Option func(*options)

func WithDisableDelay() Option {
	return func(o *options) {
		o.disableDelay = true
	}
}

func WithDisableReady() Option {
	return func(o *options) {
		o.disableReady = true
	}
}

func WithDelMsgOnCommit() Option {
	return func(o *options) {
		o.delMsgOnCommit = true
	}
}

func WithPartitionNum(num int) Option {
	return func(o *options) {
		if num > 1 {
			o.partitionNum = num
		}
	}
}

func WithPartitionOrder() Option {
	return func(o *options) {
		o.partitionOrder = true
	}
}

func WithPayloadUniqueOptimization() Option {
	return func(o *options) {
		o.payloadUniqueOptimization = true
	}
}

func WithMsgTTL(ttl int64) Option {
	return func(o *options) {
		if ttl > 1 {
			o.msgTTL = ttl
		}
	}
}

func WithLogger(logger omq.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithSleepOnEmpty(sleep time.Duration) Option {
	return func(o *options) {
		if sleep > 0 {
			o.sleepOnEmpty = sleep
		}
	}
}

func WithCommitTimeout(timeout int64) Option {
	return func(o *options) {
		if timeout > 0 {
			o.commitTimeout = timeout
		}
	}
}

func WithMaxRetry(retry int) Option {
	return func(o *options) {
		if retry >= 0 {
			o.maxRetry = retry
		}
	}
}

func WithMessageCodec(codec MessageCodec) Option {
	return func(o *options) {
		o.encodeFunc = codec.Encode
		o.decodeFunc = codec.Decode
	}
}

package redisq

import (
	"time"

	"github.com/welllog/omq"
)

type options struct {
	// Whether to disable the delayed queue or not.
	// turning it off when not using the delayed queue can reduce the overhead
	disableDelay bool

	// Message expiration time, in seconds
	msgTTL int64

	logger omq.Logger

	// The number of partitions.
	// which are randomly distributed to nodes when using a redis cluster
	partitionNum int

	// Safe mode.
	// open when the message is not submitted after a certain period of time to re-enter the queue
	safeMode bool

	// Whether to actively delete messages when submitted, only for non-safe mode
	delMsgOnCommit bool

	// Whether to use blocking pop, for non-safe mode only
	useBlockPop bool

	// Valid only when blocking is used, in seconds
	blockTimeout int

	// sleep time when there is no data in non-blocking
	sleepOnEmpty time.Duration

	// How long does it take for a message to rejoin the queue when it has not been submitted in safe mode, in seconds
	commitTimeout int
}

var defOptions = options{
	disableDelay:   false,
	msgTTL:         86400,
	logger:         omq.DefLogger{},
	partitionNum:   1,
	safeMode:       false,
	delMsgOnCommit: false,
	useBlockPop:    true,
	blockTimeout:   5,
	sleepOnEmpty:   300 * time.Millisecond,
	commitTimeout:  20,
}

type Option func(*options)

func WithDisableDelay() Option {
	return func(o *options) {
		o.disableDelay = true
	}
}

func WithPartitionNum(num int) Option {
	return func(o *options) {
		if num > 1 {
			o.partitionNum = num
		}
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

func WithSafeMode() Option {
	return func(o *options) {
		o.safeMode = true
	}
}

func WithDelMsgOnCommit() Option {
	return func(o *options) {
		o.delMsgOnCommit = true
	}
}

func WithCodisDriver() Option {
	return func(o *options) {
		o.useBlockPop = false
	}
}

func WithUseBlockPop(block bool) Option {
	return func(o *options) {
		o.useBlockPop = block
	}
}

func WithBlockTimeout(timeout int) Option {
	return func(o *options) {
		if timeout > 1 {
			o.blockTimeout = timeout
		}
	}
}

func WithSleepOnEmpty(sleep time.Duration) Option {
	return func(o *options) {
		if sleep > 0 {
			o.sleepOnEmpty = sleep
		}
	}
}

func WithCommitTimeout(timeout int) Option {
	return func(o *options) {
		if timeout > 0 {
			o.commitTimeout = timeout
		}
	}
}

package redisq

import (
	"bytes"
	"testing"
	"time"

	"github.com/welllog/omq"
)

const (
	succeed = "\u2713"
	failed  = "\u2717"
)

func TestEncodeDecode(t *testing.T) {
	tests := []omq.Message{
		{
			ID:       "1",
			Topic:    "tp",
			DelayAt:  time.Now(),
			MaxRetry: 2,
			Payload:  omq.ByteEncoder("payload"),
		},
		{
			ID:       "test",
			Topic:    "topic",
			DelayAt:  time.Time{},
			MaxRetry: 1,
			Payload:  omq.ByteEncoder{'1', ']', '\n', '\t', '\r'},
		},
		{
			Topic:   "topic2",
			DelayAt: time.Time{},
			Payload: omq.ByteEncoder{'\r', '\b', ' ', 'd'},
		},
	}
	for i := range tests {
		enc, err := encodeMsg(&tests[i])
		if err != nil {
			t.Fatalf("\t%s\t Should encode msg: %v", failed, err)
		}
		var msg omq.Message
		err = decodeMsg(enc, &msg)
		if err != nil {
			t.Fatalf("\t%s\t Should decode msg: %v, enc: %s", failed, err, enc)
		}
		if msg.ID != tests[i].ID {
			t.Fatalf("decodeMsg(%s).ID = %s, want %s", enc, msg.ID, tests[i].ID)
		}
		if msg.Topic != tests[i].Topic {
			t.Fatalf("decodeMsg(%s).Topic = %s, want %s", enc, msg.Topic, tests[i].Topic)
		}
		val1, _ := msg.Payload.Encode()
		val2, _ := tests[i].Payload.Encode()
		if !bytes.Equal(val1, val2) {
			t.Fatalf("decodeMsg(%s).Payload = %s, want %s", enc, msg.Payload, tests[i].Payload)
		}
	}
}

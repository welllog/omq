package redisq

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/welllog/omq"
)

const (
	succeed = "\u2713"
	failed  = "\u2717"
)

func TestEncodeMsg(t *testing.T) {
	at, _ := time.Parse("2006-01-02", "2022-05-06")
	tests := []omq.Message{
		{
			ID:      "1",
			Topic:   "tp",
			DelayAt: at,
			Payload: omq.ByteEncoder("payload"),
		},
		{
			ID:      "test",
			Topic:   "topic",
			DelayAt: time.Time{},
			Payload: omq.ByteEncoder{'1', ']', '\n', '\t', '\r'},
		},
	}
	for i := range tests {
		enc, err := encodeMsg(&tests[i])
		if err != nil {
			t.Fatalf("\t%s\t Should encode msg: %v", failed, err)
		}

		pb, _ := tests[i].Payload.Encode()
		expected := fmt.Sprintf("%s:%d:%s", tests[i].Topic, tests[i].DelayAt.Unix(), string(pb))
		if enc != expected {
			t.Errorf("encodeMsg(%v) = %s, want %s", tests[i], enc, expected)
		}
	}
}

func TestDecodeMsg(t *testing.T) {
	at := time.Date(2022, 5, 6, 0, 0, 0, 0, time.UTC)
	at2 := time.Unix(0, 0)
	tests := []struct {
		in  string
		out omq.Message
	}{
		{
			in:  "tp:" + strconv.FormatInt(at.Unix(), 10) + ":payload",
			out: omq.Message{Topic: "tp", DelayAt: at, Payload: omq.ByteEncoder("payload")},
		},
		{
			in:  "topic:0:1]\n\t\r",
			out: omq.Message{Topic: "topic", DelayAt: at2, Payload: omq.ByteEncoder{'1', ']', '\n', '\t', '\r'}},
		},
	}
	for i := range tests {
		var msg omq.Message
		err := decodeMsg(tests[i].in, &msg)
		if err != nil {
			t.Fatalf("\t%s\t Should decode msg: %v", failed, err)
		}
		if msg.Topic != tests[i].out.Topic {
			t.Errorf("decodeMsg(%s).Topic = %s, want %s", tests[i].in, msg.Topic, tests[i].out.Topic)
		}
		if !msg.DelayAt.Equal(tests[i].out.DelayAt) {
			t.Errorf("decodeMsg(%s).DelayAt = %s, want %s", tests[i].in, msg.DelayAt, tests[i].out.DelayAt)
		}
		val1, _ := msg.Payload.Encode()
		val2, _ := tests[i].out.Payload.Encode()
		if !reflect.DeepEqual(val1, val2) {
			t.Errorf("decodeMsg(%s).Payload = %s, want %s", tests[i].in, msg.Payload, tests[i].out.Payload)
		}
	}
}

func TestEncodeDecode(t *testing.T) {
	tests := []omq.Message{
		{
			ID:      "1",
			Topic:   "tp",
			DelayAt: time.Now(),
			Payload: omq.ByteEncoder("payload"),
		},
		{
			ID:      "test",
			Topic:   "topic",
			DelayAt: time.Time{},
			Payload: omq.ByteEncoder{'1', ']', '\n', '\t', '\r'},
		},
		{
			ID:      "test2",
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
			t.Fatalf("\t%s\t Should decode msg: %v", failed, err)
		}
		if msg.Topic != tests[i].Topic {
			t.Errorf("decodeMsg(%s).Topic = %s, want %s", enc, msg.Topic, tests[i].Topic)
		}
		if msg.DelayAt.Unix() != tests[i].DelayAt.Unix() {
			t.Errorf("decodeMsg(%s).DelayAt = %s, want %s", enc, msg.DelayAt, tests[i].DelayAt)
		}
		val1, _ := msg.Payload.Encode()
		val2, _ := tests[i].Payload.Encode()
		if !reflect.DeepEqual(val1, val2) {
			t.Errorf("decodeMsg(%s).Payload = %s, want %s", enc, msg.Payload, tests[i].Payload)
		}
	}
}

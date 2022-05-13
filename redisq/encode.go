package redisq

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/welllog/omq"
)

var errMsgInvalid = errors.New("invalid message")

func encodeMsg(msg *omq.Message) (string, error) {
	payload, err := msg.Payload.Encode()
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	buf.WriteString(msg.Topic)
	buf.WriteString(":")
	buf.WriteString(strconv.FormatInt(msg.DelayAt.Unix(), 10))
	buf.WriteString(":")
	buf.Write(payload)
	return buf.String(), nil
}

func decodeMsg(src string, msg *omq.Message) error {
	i := strings.Index(src, ":")
	if i <= 0 {
		return errMsgInvalid
	}
	msg.Topic = src[:i]
	src = src[i+1:]

	i = strings.Index(src, ":")
	if i <= 0 {
		return errMsgInvalid
	}
	timeStr := src[:i]
	timeInt, _ := strconv.ParseInt(timeStr, 10, 64)
	msg.DelayAt = time.Unix(timeInt, 0)
	msg.Payload = omq.ByteEncoder(src[i+1:])
	return nil
}

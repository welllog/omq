package redisq

import (
	"errors"
	"strings"

	"github.com/welllog/omq"
)

var (
	errMsgInvalid       = errors.New("invalid message")
	errMsgEncodeVersion = errors.New("message encode version invalid")
)

func encodeMsg(msg *omq.Message) (string, error) {
	payload, err := msg.Payload.Encode()
	if err != nil {
		return "", err
	}
	arr := []string{
		"1", msg.ID, msg.Topic,
	}
	var buf strings.Builder
	for i := range arr {
		buf.WriteString(arr[i])
		buf.WriteByte(':')
	}
	buf.Write(payload)
	return buf.String(), nil
}

func decodeMsg(src string, msg *omq.Message) error {
	origLen := len(src)
	src = strings.TrimPrefix(src, "1:")
	if len(src) == origLen {
		return errMsgEncodeVersion
	}
	arr := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		i := strings.Index(src, ":")
		if i < 0 {
			return errMsgInvalid
		}
		arr = append(arr, src[:i])
		src = src[i+1:]
	}
	msg.ID = arr[0]
	msg.Topic = arr[1]
	msg.Payload = omq.ByteEncoder(src)
	return nil
}

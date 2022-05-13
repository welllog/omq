package omq

type Encoder interface {
	Encode() ([]byte, error)
}

type PayloadEncoder struct {
	value interface{}
	codec Codec
}

func NewPayloadEncoder(value interface{}, codec Codec) *PayloadEncoder {
	return &PayloadEncoder{value: value, codec: codec}
}

func (e *PayloadEncoder) Encode() ([]byte, error) {
	return e.codec.Marshal(e.value)
}

func (e *PayloadEncoder) Value() interface{} {
	return e.value
}

type ByteEncoder []byte

func (e ByteEncoder) Encode() ([]byte, error) {
	return e, nil
}

func NewPayloadJsonEncoder(value interface{}) *PayloadEncoder {
	return NewPayloadEncoder(value, JsonCodec{})
}

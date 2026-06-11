package badger_sdk

import (
	"encoding/json"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Encoder interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

func NewEncoderByName(name string) Encoder {
	switch name {
	case "json":
		return JSONEncoder{}
	case "msgpack":
		return MsgpackEncoder{}
	case "proto":
		return ProtoEncoder{}
	default:
		return JSONEncoder{}
	}
}

type JSONEncoder struct{}

func (JSONEncoder) Marshal(v any) ([]byte, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %w", err)
	}
	return jsonData, nil
}

func (JSONEncoder) Unmarshal(data []byte, v any) error {
	err := json.Unmarshal(data, v)
	if err != nil {
		return fmt.Errorf("json.Unmarshal: %w", err)
	}
	return nil
}

type MsgpackEncoder struct{}

func (MsgpackEncoder) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (MsgpackEncoder) Unmarshal(b []byte, v any) error {
	return msgpack.Unmarshal(b, v)
}

type ProtoEncoder struct{}

func (ProtoEncoder) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("marshal not a proto.Message")
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

func (ProtoEncoder) Unmarshal(b []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("unmarshal not a proto.Message")
	}
	return proto.Unmarshal(b, m)
}

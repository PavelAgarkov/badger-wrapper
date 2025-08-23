package badger_sdk

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ProtoJsonToOutput(object proto.Message) ([]byte, error) {
	return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(object)
}

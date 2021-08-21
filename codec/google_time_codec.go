package codec

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// protobufBasicCodec кодирует/декодирует временные метки из google.protobuf.Timestamp
// в BSON Timestamp.
type protobufTimestampCodec struct {
	registry *CodecsRegistry
}

func newProtobufTimestampCodec(r *CodecsRegistry) *protobufTimestampCodec {
	return &protobufTimestampCodec{
		registry: r,
	}
}

func (pc *protobufTimestampCodec) EncodeValue(
	_ bsoncodec.EncodeContext, w bsonrw.ValueWriter, val protoreflect.Value,
) error {
	ts, ok := val.Message().Interface().(*timestamppb.Timestamp)
	if !ok {
		msgFullName := val.Message().Descriptor().FullName()
		return fmt.Errorf("message %s is not timestamppb.Timestamp", msgFullName)
	}
	return w.WriteTimestamp(uint32(ts.Seconds), uint32(ts.Nanos))
}

func (pc *protobufTimestampCodec) DecodeValue(
	_ bsoncodec.DecodeContext, r bsonrw.ValueReader, val protoreflect.Value,
) error {
	ts, ok := val.Message().Interface().(*timestamppb.Timestamp)
	if !ok {
		msgFullName := val.Message().Descriptor().FullName()
		return fmt.Errorf("message %s is not timestamppb.Timestamp", msgFullName)
	}

	secs, nanos, err := r.ReadTimestamp()
	if err != nil {
		return err
	}
	ts.Seconds = int64(secs)
	ts.Nanos = int32(nanos)

	return nil
}

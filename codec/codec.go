package codec

import (
	"fmt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"reflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
)

// ProtobufMongoCodec является адаптером, чтобы реализованные по новой нотоации
// кодеки, с новыми сигнатурами, могли удовлетворять интерфейсу bsoncodec.ValueCodec.
type ProtobufMongoCodec struct {
	Registry *CodecsRegistry
}

func NewProtobufMongoCodec() *ProtobufMongoCodec {
	return &ProtobufMongoCodec{
		Registry: defaultCodecRegistry,
	}
}

func (pc *ProtobufMongoCodec) getProtoreflectDescriptorValue(
	val reflect.Value,
) (protoreflect.Value, protoreflect.MessageDescriptor, error) {
	msg, ok := val.Interface().(proto.Message)
	if !ok {
		return protoreflect.Value{}, nil, fmt.Errorf("value must be of the proto.Message type")
	}
	reflectMsg := msg.ProtoReflect()
	return protoreflect.ValueOfMessage(reflectMsg), reflectMsg.Descriptor(), nil
}

func (pc *ProtobufMongoCodec) EncodeValue(ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val reflect.Value) error {
	msgValue, msgDescriptor, err := pc.getProtoreflectDescriptorValue(val)
	if err != nil {
		return err
	}
	codec, ok := pc.Registry.GetCodecForMessage(msgDescriptor)
	if !ok {
		return fmt.Errorf("can't find codec for %s", msgDescriptor.FullName())
	}
	return codec.EncodeValue(ctx, w, msgValue)
}

func (pc *ProtobufMongoCodec) DecodeValue(ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, val reflect.Value) error {
	msgValue, msgDescriptor, err := pc.getProtoreflectDescriptorValue(val)
	if err != nil {
		return err
	}
	codec, ok := pc.Registry.GetCodecForMessage(msgDescriptor)
	if !ok {
		return fmt.Errorf("can't find codec for %s", msgDescriptor.FullName())
	}
	return codec.DecodeValue(ctx, r, msgValue)
}

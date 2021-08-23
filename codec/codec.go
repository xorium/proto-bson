package codec

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	DefaultBSONRegistry = bson.NewRegistryBuilder().Build()
	DefaultEncContext   = bsoncodec.EncodeContext{Registry: DefaultBSONRegistry}
	DefaultDecContext   = bsoncodec.DecodeContext{Registry: DefaultBSONRegistry}
)

// ProtoValueEncoder описывает интерфейс энкодера значений Protobuf'а.
type ProtoValueEncoder interface {
	EncodeValue(ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val protoreflect.Value) error
}

// ProtoValueDecoder описывает интерфейс энкодера значений Protobuf'а.
type ProtoValueDecoder interface {
	DecodeValue(ctx bsoncodec.DecodeContext, w bsonrw.ValueReader, val protoreflect.Value) error
}

// ProtoValueCodec описывает интерфейс кодека, т.е. кодировщика-декодировщика.
type ProtoValueCodec interface {
	ProtoValueEncoder
	ProtoValueDecoder
}

// ProtobufMongoCodec является адаптером, чтобы реализованные по новой нотоации
// кодеки, с новыми сигнатурами, могли удовлетворять интерфейсу bsoncodec.ValueCodec.
// По сути, каждый метод переводит аргумент к типу proto.Message, далее с помощью
// реестра кодеков получает релевантный и испольует его для кодирования/декодирования.
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

// EncodeValue пытается сконвертировать полученное значение в proto.Message и,
// в случае успеха, опередляет следующих кодировщик, основываясь на типа сообщения
// и передавет его на кодировку ему.
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

// DecodeValue пытается сконвертировать полученное значение в proto.Message и,
// в случае успеха, опередляет следующих декодировщик, основываясь на типа сообщения
// и передавет его на кодировку ему.
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

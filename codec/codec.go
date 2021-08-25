package codec

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"reflect"

	"go.uber.org/zap"

	"go.mongodb.org/mongo-driver/bson"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
)

var (
	Logger *zap.Logger

	DefaultBSONRegistry = bson.NewRegistryBuilder().Build()
	DefaultEncContext   = bsoncodec.EncodeContext{Registry: DefaultBSONRegistry}
	DefaultDecContext   = bsoncodec.DecodeContext{Registry: DefaultBSONRegistry}
)

// ProtoValueEncoder описывает интерфейс энкодера значений Protobuf'а.
type ProtoValueEncoder interface {
	EncodeValue(ctx bsoncodec.EncodeContext, r bsonrw.ValueReader, val protoreflect.Value) error
}

// ProtoValueDecoder описывает интерфейс энкодера значений Protobuf'а.
type ProtoValueDecoder interface {
	DecodeValue(ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, val protoreflect.Value) error
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

func isEOF(err error) bool {
	switch err {
	case bsonrw.ErrEOD, bsonrw.ErrEOA, io.EOF:
		return true
	}
	return false
}

// getProtoreflectDescriptorValue возвращает protoreflect.Value для целевого
// значения сообщения, а также дескриптор этого сообщения. Будут нужны
// когда речь зайдет о том, что брать у сообщений метками самих значений.
func (pc *ProtobufMongoCodec) getProtoreflectDescriptorValue(
	val reflect.Value,
) (protoreflect.Value, protoreflect.MessageDescriptor, error) {
	msg, ok := val.Interface().(proto.Message)
	if !ok {
		return protoreflect.Value{}, nil,
		fmt.Errorf("value must be of the proto.Message type")
	}
	reflectMsg := msg.ProtoReflect()
	return protoreflect.ValueOfMessage(reflectMsg), reflectMsg.Descriptor(), nil
}

// EncodeValue пытается сконвертировать полученное значение в proto.Message и,
// в случае успеха, опередляет следующих кодировщик, основываясь на типа сообщения
// и передавет его на кодировку ему.
func (pc *ProtobufMongoCodec) EncodeValue(
	ctx bsoncodec.EncodeContext, vw bsonrw.ValueWriter,
	protoMsg reflect.Value,
) error {
	protoreflectVal, reflectMsgDescr, err := pc.getProtoreflectDescriptorValue(protoMsg)
	if err != nil {
		return err
	}
	docWriter, err := vw.WriteDocument()
	if err != nil {
		return err
	}
	for {
		valueWriter, err := docWriter.WriteDocumentElement(string(name))
		// Поля очередного сообщения проитетировались.
		if isEOF(err) {
			break
		} else if err != nil {
			Logger.Error("can't read field from BSON message",
				zap.String("field", name), zap.Error(err))
			continue
		}
		// Подбираем конвертер для значения val, чтобы оно безконфликтно
		// сохранилось в w.
		_, msgDescriptor, err := pc.getProtoreflectDescriptorValue(val)
		if err != nil {
			Logger.Error("can't protoreflect.Value for target `value`",
				zap.Error(err))
			continue
		}
		// Получение релевантного кодека, который будет преобразовывать данные.
		// Это попытка получить кодек из зареганых юзерами, т.к. они приоритете.
		codec, ok := pc.Registry.GetCodecForMessage(msgDescriptor)
		if !ok {
			return fmt.Errorf("can't find codec for %s", msgDescriptor.FullName())
		}
		codec, ok = pc.Registry.GetCodecByValue(protoreflect.ValueOf(val.Interface()))
		if !ok {
			return fmt.Errorf("can't find codec by interface{}")
		}
		codec = pc.Registry.GetCodecByBSONType(valueReader.Type())

		if err := codec.EncodeValue(ctx, w, msgValue); err != nil {
			return err
		}
	}
	return nil
}

// DecodeValue пытается сконвертировать полученное значение в proto.Message и,
// в случае успеха, опередляет следующих декодировщик, основываясь на типа сообщения
// и передавет его на кодировку ему.
func (pc *ProtobufMongoCodec) DecodeValue(ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, protoMsgVal reflect.Value) error {
	msgValue, msgDescriptor, err := pc.getProtoreflectDescriptorValue(protoMsgVal)
	if err != nil {
		return err
	}
	codec, ok := pc.Registry.GetCodecForMessage(msgDescriptor)
	if !ok {
		return fmt.Errorf("can't find codec for %s", msgDescriptor.FullName())
	}
	return codec.DecodeValue(ctx, r, msgValue)
}

// BSONToProto шорткат для удобной конвертации из BSON в Protobuf.
func BSONToProto(bsonData []byte, msg proto.Message) error {
	codec := NewProtobufMongoCodec()
	buffer := bufio.NewWriter(bytes.NewBuffer(bsonData))
	writer, err := bsonrw.NewBSONValueWriter(buffer)
	if err != nil {
		return err
	}
	return codec.EncodeValue(DefaultEncContext, writer, reflect.ValueOf(msg))
}

// ProtoToBSON - шорткат для удобной конвертации из Protobuf в BSON.
func ProtoToBSON(bsonData []byte, msg proto.Message) error {
	codec := NewProtobufMongoCodec()
	reader := bsonrw.NewBSONDocumentReader(bsonData)

	listValue := msg.List()
	if !listValue.IsValid() {
		return fmt.Errorf("list value %v is invalid", val)
	}

	for listValue.IsValid() {
		valueReader, err := reader.ReadValue()
		switch err {
		case nil:
		case bsonrw.ErrEOA, bsonrw.ErrEOD, io.EOF:
			return nil
		default:
			log.Println("Can't read list element value, err=%", err)
		}
		listItem := listValue.NewElement()
		codec, ok := pc.registry.GetCodecByValue(listItem)
		if ok {
			if err = codec.DecodeValue(ctx, valueReader, listItem); err != nil {
				return err
			}
		} else {
			basicValType := reflect.TypeOf(listItem.Interface())
			basicValue, err := pc.registry.BasicCodec.DecodeValue(ctx, valueReader, basicValType)
			if err != nil {
				log.Println("Can't decode value", basicValue)
				continue
			}
			listItem = protoreflect.ValueOf(basicValue)
		}
		listValue.Append(listItem)
	}Value(DefaultDecContext, reader, reflect.ValueOf(msg))
}
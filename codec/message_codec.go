package codec

import (
	"fmt"
	"log"
	"reflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// protobufMessageCodec кодек для сообщений Protobuf'а.
type protobufMessageCodec struct {
	registry *CodecsRegistry
}

func newProtobufMessageCodec(r *CodecsRegistry) *protobufMessageCodec {
	return &protobufMessageCodec{
		registry: r,
	}
}

func (pc *protobufMessageCodec) fieldKey(field protoreflect.FieldDescriptor) string {
	return fmt.Sprintf("%d", field.Number())
}

func (pc *protobufMessageCodec) getMessageFields(msg proto.Message) map[string]protoreflect.FieldDescriptor {
	fields := make(map[string]protoreflect.FieldDescriptor)
	reflectMessage := msg.ProtoReflect()
	msgDescriptor := reflectMessage.Descriptor()

	// Сначала собираем все непустые oneof поля.
	oneOfs := msgDescriptor.Oneofs()
	for i := 0; i < oneOfs.Len(); i++ {
		oneof := oneOfs.Get(i)
		field := reflectMessage.WhichOneof(oneof)
		fields[pc.fieldKey(field)] = field
	}
	// Затем - все остальные поля.
	commonFields := msgDescriptor.Fields()
	for i := 0; i < commonFields.Len(); i++ {
		field := commonFields.Get(i)
		// Если это поле - одно из значений oneof'а - пропускаем его, т.к. оно
		// уже добавлено в слайс.
		if field.ContainingOneof() != nil {
			continue
		}
		fields[pc.fieldKey(field)] = field
	}

	return fields
}

func (pc *protobufMessageCodec) EncodeValue(
	ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val protoreflect.Value,
) error {
	msg := val.Message().Interface()
	reflectMsg := msg.ProtoReflect()

	dw, err := w.WriteDocument()
	if err != nil {
		return err
	}

	for _, field := range pc.getMessageFields(msg) {
		value := reflectMsg.Get(field)
		if !value.IsValid() {
			continue
		}
		writer, err := dw.WriteDocumentElement(pc.fieldKey(field))
		if err != nil {
			return err
		}
		codec, ok := pc.registry.GetCodecByField(field)
		if ok {
			err = codec.EncodeValue(ctx, writer, value)
		} else {
			err = pc.registry.BasicCodec.EncodeValue(ctx, writer, value)
		}
		if err != nil {
			return err
		}
	}

	return dw.WriteDocumentEnd()
}

func (pc *protobufMessageCodec) getAllMessageFields(msg proto.Message) map[string]protoreflect.FieldDescriptor {
	fields := make(map[string]protoreflect.FieldDescriptor)
	reflectMessage := msg.ProtoReflect()
	msgDescriptor := reflectMessage.Descriptor()

	commonFields := msgDescriptor.Fields()
	for i := 0; i < commonFields.Len(); i++ {
		field := commonFields.Get(i)
		fields[pc.fieldKey(field)] = field
	}

	return fields
}

func (pc *protobufMessageCodec) DecodeValue(
	ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, val protoreflect.Value,
) error {
	msg := val.Message().Interface()
	reflectMsg := msg.ProtoReflect()

	docReader, err := r.ReadDocument()
	if err != nil {
		return err
	}

	msgFieldsMap := pc.getAllMessageFields(msg)

	for {
		strKey, valueReader, err := docReader.ReadElement()
		fmt.Printf("[msg] reading '%s' field err <%v>.\n", strKey, err)
		if err == bsonrw.ErrEOD {
			break
		} else if err != nil {
			return err
		}

		field, ok := msgFieldsMap[strKey]
		if !ok {
			log.Printf(
				"Can't find field %s of %s.\n",
				strKey, reflectMsg.Descriptor().FullName(),
			)
			continue
		}

		codec, ok := pc.registry.GetCodecByField(field)
		value := reflectMsg.NewField(field)
		if ok {
			err := codec.DecodeValue(ctx, valueReader, value)
			if err != nil {
				return err
			}
		} else {
			basicValType := reflect.TypeOf(value.Interface())
			basicVal, err := pc.registry.BasicCodec.DecodeValue(ctx, valueReader, basicValType)
			if err != nil {
				return err
			}
			value = protoreflect.ValueOf(basicVal)
		}

		reflectMsg.Set(field, value)
	}

	return nil
}

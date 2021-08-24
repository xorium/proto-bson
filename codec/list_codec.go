package codec

import (
	"fmt"
	"log"
	"reflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// protobufListCodec кодирует/декодирует списочные типы Protobuf сообщений.
type protobufListCodec struct {
	registry *CodecsRegistry
}

func newProtobufListCodec(r *CodecsRegistry) *protobufListCodec {
	return &protobufListCodec{
		registry: r,
	}
}

func (pc *protobufListCodec) EncodeValue(
	ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val protoreflect.Value,
) error {
	writer, err := w.WriteArray()
	if err != nil {
		return err
	}
	defer func() {
		if err = writer.WriteArrayEnd(); err != nil {
			panic(err)
		}
	}()

	listValue := val.List()
	if !listValue.IsValid() {
		log.Printf("list value %v is invalid", val)
		return nil
	}

	for i := 0; i < listValue.Len(); i++ {
		fmt.Println("ITERATION ", i+1)
		listItem := listValue.Get(i)
		if !listItem.IsValid() {
			continue
		}
		if err != nil {
			return err
		}
		valueWriter, err := writer.WriteArrayElement()
		if err != nil {
			return err
		}
		codec, ok := pc.registry.GetCodecByValue(listItem)
		if ok {
			err = codec.EncodeValue(ctx, valueWriter, listItem)
		} else {
			err = pc.registry.BasicCodec.EncodeValue(ctx, valueWriter, listItem)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (pc *protobufListCodec) DecodeValue(
	ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, val protoreflect.Value,
) error {
	reader, err := r.ReadArray()
	if err != nil {
		return err
	}

	listValue := val.List()
	if !listValue.IsValid() {
		return fmt.Errorf("list value %v is invalid", val)
	}

	for {
		valueReader, err := reader.ReadValue()
		if err != nil && err != bsonrw.ErrEOA || valueReader == nil || valueReader.Type() == 0 {
			readerType := "unknown"
			if valueReader != nil {
				readerType = valueReader.Type().String()
			}
			log.Printf(
				"Can't read list element value, valReader.Type()=%v, err=%v.\n",
				readerType, err,
			)
		}
		if err == bsonrw.ErrEOA {
			break
		} else if err != nil {
			return err
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
	}

	return nil
}

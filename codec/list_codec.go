package codec

import (
	"fmt"
	"go.uber.org/zap"
	"io"
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
			Logger.Error("can't ending writing some BSON array", zap.Error(err))
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
	ctx bsoncodec.DecodeContext, r bsonrw.DocumentReader, val protoreflect.Value,
) error {
	for {
		elemName, valueReader, err := r.ReadElement()
		valueReader.
		if err != nil {
			if err != bsonrw.ErrEOA
		}
		reader, err := r.ReadArray()
		if err != nil {
			return err
		}

		listValue := val.List()
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
		}
	}

	return nil
}

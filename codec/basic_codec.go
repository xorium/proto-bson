package codec

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
	"reflect"
)

// protobufBasicCodec кодирует/декодирует базовые, фундаментальные типы данных:
// числовые, числовые с плавающей запятой, строки, массивы из простых типов.
type protobufBasicCodec struct {
	// Реестра типов-синонимов, который нужно конвертировать в свой синоним перед
	// декодированием и кодирова - после. Например, protobuf чилос enum'в в int32
	// и - обратно.
	convertableTypes map[reflect.Type]reflect.Type
	// Реестра более сложных (но все еще не осставных типов, напрпимер, enum'ы.)
	registry *CodecsRegistry
	// Контексты приобразования базовых типов для BSON'а.
	basicEncCtx bsoncodec.EncodeContext
	basicDecCtx bsoncodec.DecodeContext
}

func newProtobufBasicCodec(r *CodecsRegistry) *protobufBasicCodec {
	return &protobufBasicCodec{
		registry:    r,
		basicEncCtx: DefaultEncContext,
		basicDecCtx: DefaultDecContext,
		convertableTypes: map[reflect.Type]reflect.Type{
			reflect.TypeOf(protoreflect.EnumNumber(0)): reflect.TypeOf(int32(0)),
		},
	}
}

func (pc *protobufBasicCodec) EncodeValue(
	ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val protoreflect.Value,
) error {
	encoder, err := ctx.LookupEncoder(reflect.TypeOf(val.Interface()))
	if err != nil {
		return err
	}
	return encoder.EncodeValue(ctx, w, reflect.ValueOf(val.Interface()))
}

// createWithCanonicalType создает пустое значение при этом заранее проверяя, может
// ли быть тип t приветед к более одному из баховых (int, string) и, если да, то
// осуществляет конвертацию создаваемого значения из специфичного типа t в базовый.
func (pc *protobufBasicCodec) createWithCanonicalType(t reflect.Type) (val reflect.Value, wasConverted bool) {
	value := reflect.New(t).Elem()
	baseType, convertable := pc.convertableTypes[t]
	if convertable {
		newValue := reflect.New(baseType).Elem()
		newValue.Set(value.Convert(baseType))
		return newValue, true
	}
	return value, false
}

func (pc *protobufBasicCodec) DecodeValue(
	ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, valType reflect.Type,
) (interface{}, error) {
	decoder, err := ctx.LookupDecoder(valType)
	if err != nil {
		return nil, err
	}

	switch valType.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Interface:
		return nil, fmt.Errorf("basic types only are supported")
	}

	value, wasConverted := pc.createWithCanonicalType(valType)
	if err = decoder.DecodeValue(ctx, r, value); err != nil {
		return nil, err
	}
	// Конвертируем обратно в специфичный тип данных valType.
	if wasConverted {
		value = value.Convert(valType)
	}

	return value.Interface(), err
}

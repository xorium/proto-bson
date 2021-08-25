package codec

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"log"
	"reflect"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var basicReflectTypesByKind = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeOf(0),
	reflect.Int8:    reflect.TypeOf(int8(0)),
	reflect.Int16:   reflect.TypeOf(int16(0)),
	reflect.Int32:   reflect.TypeOf(int32(0)),
	reflect.Int64:   reflect.TypeOf(int64(0)),
	reflect.Uint:    reflect.TypeOf(uint(0)),
	reflect.Uint8:   reflect.TypeOf(uint8(0)),
	reflect.Uint16:  reflect.TypeOf(uint16(0)),
	reflect.Uint32:  reflect.TypeOf(uint32(0)),
	reflect.Uint64:  reflect.TypeOf(uint64(0)),
	reflect.Float32: reflect.TypeOf(float32(0)),
	reflect.Float64: reflect.TypeOf(float64(0)),
	reflect.Bool:    reflect.TypeOf(false),
	reflect.String:  reflect.TypeOf(""),
}

// protobufMapCodec кодирует/декодирует Map значения Protobuf сообщений.
// Поддерживает не только строковые ключи, ключом может быть любой базовый тип
// данных Protobuf'а.
type protobufMapCodec struct {
	registry      *CodecsRegistry
	keysDelimiter string
}

func newProtobufMapCodec(r *CodecsRegistry) *protobufMapCodec {
	return &protobufMapCodec{
		registry:      r,
		keysDelimiter: "|",
	}
}

func (pc *protobufMapCodec) encodeMapKey(key protoreflect.MapKey) (string, error) {
	keyValue := key.Interface()

	// Если ключ уже в строковом формате, то нет необходимости его проводить по
	// общему алгоритму приведения к строковому виду.
	if _, ok := keyValue.(string); ok {
		return keyValue.(string), nil
	}

	keyData, err := json.Marshal(key.Interface())
	if err != nil {
		return "", err
	}
	keyReflectKindCode := reflect.TypeOf(key.Interface()).Kind()
	// Ключи у мап разрешены только базовых типов.
	if _, ok := basicReflectTypesByKind[keyReflectKindCode]; !ok {
		return "", fmt.Errorf(
			"can't encode key %v: unsupported type %s",
			key.Interface(), keyReflectKindCode.String(),
		)
	}
	// Ключ будет включать в себя сами данные, разделитель и код вида типа данных
	// оригинального ключа.
	return fmt.Sprintf("%s%s%d", string(keyData), pc.keysDelimiter, keyReflectKindCode), nil
}

func (pc *protobufMapCodec) decodeMapKey(strKey string) protoreflect.MapKey {
	// Разбиваем данные ключа на части - в левой будет сам JSON сериализованный
	// ключ, а в правой - идентификатор типа значения данного ключа.
	delimiterIndex := strings.LastIndex(strKey, pc.keysDelimiter)
	// Если разделитель не найден, значит ключ изначально был в строковом виде,
	// его не нужно приводить и можно вернуть в таком виде.
	if delimiterIndex == -1 {
		return protoreflect.ValueOf(strKey).MapKey()
	}
	keyData := strKey[:delimiterIndex]
	keyReflectKindCode, err := strconv.Atoi(strKey[delimiterIndex+1:])
	// Если идентификатор типа не распарсился, значит ключ изначально был в другом
	// - в строковом формате - можно вернуть его, как есть.
	if err != nil {
		return protoreflect.ValueOf(strKey).MapKey()
	}

	keyType := basicReflectTypesByKind[reflect.Kind(keyReflectKindCode)]
	key := reflect.New(keyType)
	// Если произошла ошибка при анмаршалинге, значит ключ изначально был в строковом
	// значении - возвращаем его, как есть.
	if err := json.Unmarshal([]byte(keyData), key.Interface()); err != nil {
		return protoreflect.ValueOf(strKey).MapKey()
	}

	return protoreflect.ValueOf(key.Elem().Interface()).MapKey()
}

func (pc *protobufMapCodec) EncodeValue(
	ctx bsoncodec.EncodeContext, docMap bsonrw.DocumentWriter, val protoreflect.Value,
) error {
	mapValue := val.Map()
	if !mapValue.IsValid() {
		return fmt.Errorf("map value is invalid: %v", mapValue)
	}

	mapValue.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		strKey, err := pc.encodeMapKey(key)
		if err != nil {
			log.Println("Can't encode key", key)
			return false
		}
		valueWriter, err := docMap.WriteDocumentElement(strKey)
		if err != nil {
			log.Println("Can't write document element by key", strKey)
			return false
		}

		codec, ok := pc.registry.GetCodecByValue(value)
		if ok {
			err = codec.EncodeValue(ctx, docMap, value)
		} else {
			err = pc.registry.BasicCodec.EncodeValue(ctx, valueWriter, value)
		}
		if err != nil {
			log.Println("Can't encode value", value)
			return false
		}

		return true
	})

	return nil
}

func (pc *protobufMapCodec) DecodeValue(
	ctx bsoncodec.DecodeContext, mapReader bsonrw.DocumentReader, val protoreflect.Value,
) error {
	mapValue := val.Map()
	if !mapValue.IsValid() {
		return fmt.Errorf("map value is invalid: %v", mapValue)
	}

	for {
		strKey, valueReader, err := mapReader.ReadElement()
		Logger.Info("From Proto map to BSON doc iteration",
			zap.Error(err),
			zap.String("key", strKey))
		if err != nil {
			break
		}

		mapKey := pc.decodeMapKey(strKey)
		value := mapValue.NewValue()
		codec, ok := pc.registry.GetCodecByValue(value)
		// Если нашли кодек для нужного типа значения мапы, то используем его.
		if ok {
			if err = codec.DecodeValue(ctx, valueReader, value); err != nil {
				log.Println("Can't decode value", value)
				continue
			}
		} else {
			// Иначе - исползуем стандартный кодеровщик базовых типов.
			basicValType := reflect.TypeOf(value.Interface())
			basicValue, err := pc.registry.BasicCodec.DecodeValue(ctx, valueReader, basicValType)
			if err != nil {
				log.Println("Can't decode value", basicValue)
				continue
			}
			value = protoreflect.ValueOf(basicValue)
		}

		mapValue.Set(mapKey, value)
	}

	return nil
}

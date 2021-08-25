package codec

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"reflect"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"
)

var defaultCodecRegistry = DefaultCodecsRegistry()

const (
	ProtobufKindList      = "protoreflect.List"
	ProtobufKindMap       = "protoreflect.Map"
	ProtobufKindMessage   = "protoreflect.Message"
	ProtobufKindTimestamp = "google.protobuf.Timestamp"
)

// CodecsRegistry - реестр кодеков различных видов значений Protobuf'а.
// Также помимо обобщенных видов в нем можно регистрировать конкретные типы
// сообщений Protobuf'а, например, google.protobuf.Timestamp.
type CodecsRegistry struct {
	*sync.RWMutex
	registry map[string]ProtoValueCodec

	BasicCodec *protobufBasicCodec
}

func NewCodecsRegistry() *CodecsRegistry {
	r := &CodecsRegistry{
		RWMutex:  new(sync.RWMutex),
		registry: make(map[string]ProtoValueCodec),
	}
	r.BasicCodec = newProtobufBasicCodec(r)
	return r
}

func DefaultCodecsRegistry() *CodecsRegistry {
	r := NewCodecsRegistry()

	_ = r.RegisterCodec(ProtobufKindMap, newProtobufMapCodec(r))
	_ = r.RegisterCodec(ProtobufKindList, newProtobufListCodec(r))
	_ = r.RegisterCodec(ProtobufKindMessage, newProtobufMessageCodec(r))
	_ = r.RegisterCodec(ProtobufKindTimestamp, newProtobufTimestampCodec(r))

	return r
}

func (r *CodecsRegistry) RegisterCodec(typeName string, codec ProtoValueCodec) error {
	r.Lock()
	defer r.Unlock()
	if currCodec, ok := r.registry[typeName]; ok && codec != currCodec {
		return fmt.Errorf("there is another codec already registered")
	}
	r.registry[typeName] = codec
	return nil
}

func (r *CodecsRegistry) GetCodec(typeName string) (ProtoValueCodec, bool) {
	r.RLock()
	defer r.RUnlock()
	codec, ok := r.registry[typeName]
	return codec, ok
}

func (r *CodecsRegistry) GetCodecByBSONType(t bsontype.Type) ProtoValueCodec {
	switch t {
	case bsontype.Array:
		return r.registry[ProtobufKindList]
	case bsontype.EmbeddedDocument:
		return r.registry[ProtobufKindList]
	}
	return nil
}

// GetCodecByField основываясь на типе поля, возвращает подходящий кодек, если
// он был зарегистрирован для этого типа поля.
func (r *CodecsRegistry) GetCodecByField(
	field protoreflect.FieldDescriptor,
) (codec ProtoValueCodec, ok bool) {
	switch {
	case field.IsList():
		codec, ok = r.GetCodec(ProtobufKindList)
	case field.IsMap():
		codec, ok = r.GetCodec(ProtobufKindMap)
	case field.Kind() == protoreflect.MessageKind:
		codec, ok = r.GetCodecForMessage(field.Message())
	case field.Kind() == protoreflect.GroupKind:
		codec, ok = r.GetCodecForMessage(field.Message())
	}
	return codec, ok
}

// GetCodecByValue основываясь на типе значения, возвращает подходящий кодек, если
// он был зарегистрирован для этого типа поля.
func (r *CodecsRegistry) GetCodecByValue(value protoreflect.Value) (codec ProtoValueCodec, ok bool) {
	if !value.IsValid() {
		return nil, false
	}
	// TODO: усптростить детект типа значения value protoreflect.Value.
	isValueMessage := func(val protoreflect.Value) (isMessage bool) {
		defer func() {
			if r := recover(); r != nil {
				isMessage = false
			}
		}()
		val.Message()
		isMessage = true
		return isMessage
	}
	isValueList := func(val protoreflect.Value) (isList bool) {
		defer func() {
			if r := recover(); r != nil {
				isList = false
			}
		}()
		val.List()
		isList = true
		return isList
	}
	isValueMap := func(val protoreflect.Value) (isMap bool) {
		defer func() {
			if r := recover(); r != nil {
				isMap = false
			}
		}()
		val.Map()
		isMap = true
		return isMap
	}

	switch {
	case isValueMessage(value):
		codec, ok = r.GetCodecForMessage(value.Message().Descriptor())
	case isValueList(value):
		codec, ok = r.GetCodec(ProtobufKindList)
	case isValueMap(value):
		_, ok = value.Interface().(protoreflect.Map)
		fmt.Println(ok)
		if ok {
			protoreflectMapType := reflect.TypeOf(protoreflect.Map(nil))
			isMap := reflect.TypeOf(value.Interface()).Implements(protoreflectMapType)
			fmt.Println(isMap)
		}
		codec, ok = r.GetCodec(ProtobufKindMap)
	}

	return codec, ok
}

// GetCodecForMessage возвращает кодек для Protobuf сообщения msg, приоритетно
// пытаясь получить кодек по конкретному типу сообщения и в случае неудачи
// пытается вернуть общий кодек для сообщений.
func (r *CodecsRegistry) GetCodecForMessage(
	msg protoreflect.MessageDescriptor,
) (ProtoValueCodec, bool) {
	// Если в реестре нашелся конкретный зарегистрирвоанный кодек для данного
	// типа сообщения, то используем его, вместо общего кодека для сообщений.
	if specificCodec, ok := r.GetCodec(string(msg.FullName())); ok {
		return specificCodec, true
	}
	codec, ok := r.GetCodec(ProtobufKindMessage)
	return codec, ok
}

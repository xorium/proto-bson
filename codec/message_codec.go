package codec

import (
	"bytes"
	"reflect"

	"go.uber.org/zap"

	pref "google.golang.org/protobuf/reflect/protoreflect"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
)

func EncodeDocument(doc interface{}) ([]byte, error) {
	bsonBuf := bytes.NewBuffer(nil)
	writer, err := bsonrw.NewBSONValueWriter(bsonBuf)
	if err != nil {
		return nil, err
	}
	docWriter, err := writer.WriteDocument()
	if err != nil {
		return nil, err
	}
	pc := NewProtobufMongoCodec()
	if err := pc.EncodeValue(DefaultEncContext, docWriter, reflect.ValueOf(doc)); err != nil {
		return nil, err
	}
	return bsonBuf.Bytes(), nil
}

func DecodeDocument(doc interface{}) (err error) {
	bsonBytes, err := EncodeDocument(doc)
	if err != nil {
		return err
	}

	reader := bsonrw.NewBSONDocumentReader(bsonBytes)
	pc := NewProtobufMongoCodec()
	err = pc.DecodeValue(DefaultDecContext, reader, reflect.ValueOf(doc))
	if err != nil {
		return err
	}
	return nil
}

// protobufMessageCodec кодек для сообщений Protobuf'а.
type protobufMessageCodec struct {
	registry *CodecsRegistry
}

func newProtobufMessageCodec(r *CodecsRegistry) *protobufMessageCodec {
	return &protobufMessageCodec{
		registry: r,
	}
}

func (pc *protobufMessageCodec) fieldKey(field pref.FieldDescriptor) string {
	return string(field.Name())
}

func (pc *protobufMessageCodec) getMessageFields(msg proto.Message) map[string]pref.FieldDescriptor {
	fields := make(map[string]pref.FieldDescriptor)
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
	ctx bsoncodec.EncodeContext, w bsonrw.ValueWriter, val pref.Value,
) error {
	msg := val.Message().Interface()
	reflectMsg := msg.ProtoReflect()

	dw, err := w.WriteDocument()
	if err != nil {
		return err
	}
	func() { _ = dw.WriteDocumentEnd() }()

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
	return nil
}

func (pc *protobufMessageCodec) getAllMessageFields(msg proto.Message) map[string]pref.FieldDescriptor {
	fields := make(map[string]pref.FieldDescriptor)
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
	ctx bsoncodec.DecodeContext, r bsonrw.ValueReader, val pref.Value,
) error {
	Logger.Debug("Start decoding into message document", zap.Field{Key: "main"})

	msg := val.Message().Interface()
	reflectMsg := msg.ProtoReflect()
	msgFieldsMap := pc.getAllMessageFields(msg)
	msgName := string(reflectMsg.Descriptor().FullName())

	Logger.Debug(
		"Starting iteration over all msg fields",
		zap.Field{Key: "MSG", Interface: msg},
	)
	docReader, err := r.ReadDocument()
	if err != nil {
		return err
	}

	for {
		strKey, valueReader, err := docReader.ReadElement()
		if err != bsonrw.ErrEOD && err != nil {
			break
		}
		var field, ok = msgFieldsMap[strKey]
		if !ok {
			Logger.Debug(
				"Can't find field for such bson key", zap.String("msg", msgName),
				zap.String("key", strKey),
			)
			continue
		}
		Logger.Debug("Stopping iteration cause of OEF err.", zap.String("Object key", strKey))

		// Получение очередного поля документа.
		field, ok = msgFieldsMap[strKey]
		fieldName := string(field.FullName())
		if !ok {
			Logger.Warn("Can't get field by name.", zap.String("Msg", msgName), zap.String("fieldKey", strKey))
			continue
		}
		// Поиск кодека и приведение значения.
		codec, ok := pc.registry.GetCodecByField(field)
		value := reflectMsg.NewField(field)
		// Если кодек был найдет, то испоьзуется он, иначе - кодек для базовых типов.
		if ok {
			Logger.Debug("Found special codec for field ", zap.String("field", fieldName))
			err = codec.DecodeValue(ctx, valueReader, value)
			if err != nil {
				Logger.Error("Can't save value into field.", zap.String("field", fieldName), zap.Any("valueReader", valueReader), zap.Error(err))
				continue
			}
		} else {
			Logger.Info("Found basic codec for field ", zap.String("field", fieldName), zap.Any("value", value.Interface()), zap.Reflect("valType", reflect.TypeOf(value.Interface())))
			basicValType := reflect.TypeOf(value.Interface())
			basicVal, err := pc.registry.BasicCodec.DecodeValue(ctx, valueReader, basicValType)

			if err != nil {
				Logger.Error("Can't save value into field.", zap.String("field", fieldName), zap.Any("valueReader", valueReader), zap.Error(err))
				continue
			}
			value = pref.ValueOf(basicVal)
		}
		reflectMsg.Set(field, value)
	}
	return nil
}

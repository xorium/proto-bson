package main

import (
	"bytes"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/types/known/timestamppb"
	"proto-encoder/codec"
	"proto-encoder/gen"
	"reflect"
)

func main() {
	msg := &gen.Example{
		StringField:  "test123",
		EnumField:    gen.ExampleEnum_VAL_1,
		ExampleOneof: &gen.Example_Int32Field{Int32Field: 32},
		NestedMessage: &gen.NestedMessage{
			NestedStringField: "nested123",
			NestedInt32Field:  123,
		},
		Projects: map[string]bool{
			"abd": false,
			"def": true,
		},
		StrArray: []string{"a", "b", "c"},
		Ts:       timestamppb.Now(),
	}

	buf := bytes.NewBuffer(nil)
	writer, err := bsonrw.NewBSONValueWriter(buf)
	protoCodec := codec.NewProtobufMongoCodec()

	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf(msg), protoCodec)
	rb.RegisterDefaultEncoder(reflect.Struct, protoCodec)
	rb.RegisterDefaultDecoder(reflect.Struct, protoCodec)
	reg := rb.Build()

	ectx := bsoncodec.EncodeContext{Registry: reg}
	err = protoCodec.EncodeValue(ectx, writer, reflect.ValueOf(msg))
	if err != nil {
		panic(err)
	}

	fmt.Println(msg)

	handleRead(bsonrw.NewBSONValueReader(bson.TypeEmbeddedDocument, buf.Bytes()))
}

func handleRead(r bsonrw.ValueReader) {
	msgCopy := new(gen.Example)
	protoCodec := codec.NewProtobufMongoCodec()

	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf(msgCopy), protoCodec)
	rb.RegisterDefaultEncoder(reflect.Struct, protoCodec)
	rb.RegisterDefaultDecoder(reflect.Struct, protoCodec)
	reg := rb.Build()

	ectx := bsoncodec.DecodeContext{Registry: reg}
	value := reflect.New(reflect.TypeOf(gen.Example{}))
	err := protoCodec.DecodeValue(ectx, r, value)
	if err != nil {
		panic(err)
	}
	fmt.Println(err)
}

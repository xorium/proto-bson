package main

import (
	"bytes"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
	"proto-encoder/codec"
	"reflect"
)

func main() {
	msg := struct{}{}

	buf := bytes.NewBuffer(nil)
	w, _ := bsonrw.NewBSONValueWriter(buf)

	rb := bson.NewRegistryBuilder()
	reg := rb.Build()

	c := codec.NewProtobufMongoCodec()

	rb.RegisterCodec(reflect.TypeOf(protoreflect.Message(nil)), c)
	fmt.Println(123)
	fmt.Println(rb)

	err := c.EncodeValue(bsoncodec.EncodeContext{Registry: reg}, w, reflect.ValueOf(msg))
	if err != nil {
		panic(err)
	}
}

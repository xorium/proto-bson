package main

import (
	"bytes"
	"reflect"

	"bitbucket.org/entrlcom/proto-mongo/codec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

func main() {
	msg := struct{}{}

	buf := bytes.NewBuffer(nil)
	w, _ := bsonrw.NewBSONValueWriter(buf)

	rb := bson.NewRegistryBuilder()
	reg := rb.Build()

	c := codec.NewProtobufMongoCodec()
	err := c.EncodeValue(bsoncodec.EncodeContext{Registry: reg}, w, reflect.ValueOf(msg))
	if err != nil {
		panic(err)
	}
}

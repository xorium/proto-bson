package codec

import (
	"bytes"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"proto-encoder/gen"
	"reflect"
	"testing"
)

func ProtoBufCustomCodecRegister(t *testing.T) {
	protoCodec := NewProtobufMongoCodec()
	protoCodec.Registry = NewCodecsRegistry()

	msg1 := &gen.Example{
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
	msg2 := &gen.NestedMessage{}

	dadaBuffer := bytes.NewBuffer(nil)

	rb := bson.NewRegistryBuilder()
	rb.RegisterCodec(reflect.TypeOf(msg1.ProtoReflect()), codec1)

	reg := rb.Build()

	w, _ := bsonrw.NewBSONValueWriter(dadaBuffer)

	msg1.ProtoReflect()
	fmt.Println(t1, t2, t3, t4)

	targetCodec, err := reg.LookupEncoder(reflect.TypeOf(&NestedMessage{}))
	if err != nil {
		fmt.Println("Can't get encoder for Example message:", err)
	}
	fmt.Println(targetCodec)

	rb.RegisterCodec(reflect.TypeOf(protoreflect.Message(nil)), codec1)
	fmt.Println(123)
	fmt.Println(rb)

	err = codec1.EncodeValue(bsoncodec.EncodeContext{Registry: reg}, w, reflect.ValueOf(msg1))
	if err != nil {
		panic(err)
	}

}

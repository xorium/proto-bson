package main

func main() {

}

/*
func example() {
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

	bsonMsg, err:= codec.EncodeProto(msg)
	if err != nil {
		panic(err)
	}

	m := make(map[string]interface{})
	err = bson.Unmarshal(bsonMsg, &m)
	if err != nil {
		panic(err)
	}
	fmt.Println(m)

	msgNew := new(gen.Example)
	if err = codec.DecodeBSON(msgNew, bsonMsg); err != nil {
		panic(err)
	}
	fmt.Println(msgNew)
}
*/

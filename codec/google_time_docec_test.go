package codec

import (
	a "bitbucket.org/entrlcom/genproto/gen/go/account/v1"
	"bytes"
	"fmt"
	asrt "github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"testing"
)

var account = &a.Account{
	Created: timestamppb.Now(),
	Identities: []*a.Identity{{
		IdentityClaims: &a.IdentityClaims{
			IdentityClaims: &a.IdentityClaims_YandexIdentityClaims{
				YandexIdentityClaims: &a.YandexIdentityClaims{
					Birthdate: timestamppb.Now(),
					Email:     "john.doe@gmail.com",
					FirstName: "John",
					Id:        "1",
					LastName:  "Doe",
					Phone:     "",
					Picture:   "",
					Sex:       a.Sex_SEX_MALE,
					Username:  "",
				},
			},
		},
		LastLogin: timestamppb.Now(),
		Provider:  a.Provider_PROVIDER_YANDEX,
	}},
	LastLogin:  timestamppb.Now(),
	Name:       "John Doe",
	PictureUri: "",
	Status:     a.AccountStatus_ACCOUNT_STATUS_UNSPECIFIED,
	Username:   "j.doe",
}

func TestEncodeDecode(t *testing.T) {
	assert := asrt.New(t)

	bsonData, err := EncodeDocument(account)
	assert.Nil(err)
	assert.True(len(bsonData) > 0, "marshalled BSON bytes must be full.")

	acc := &account
	err = DecodeDocument(acc)
	assert.Nil(err, "error while decoding BSON data for account")
	fmt.Println(acc)
}

func TestAccountTimestampDecoding(t *testing.T) {
	acc := account
	c := NewProtobufMongoCodec()
	w, err := bsonrw.NewBSONValueWriter(bytes.NewBuffer([]byte{}))
	if err != nil {
		t.Fatal(err)
	}
	err = c.EncodeValue(DefaultEncContext, w, reflect.ValueOf(acc))
	if err != nil {
		t.Fatal(err)
	}
}

func TestAccountAccountDecoding(t *testing.T) {
	c := NewProtobufMongoCodec()

	buf := bytes.NewBuffer(nil)
	w, err := bsonrw.NewBSONValueWriter(buf)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.WriteDocument()
	if err != nil {
		t.Fatal(err)
	}
	err = c.EncodeValue(DefaultEncContext, w, reflect.ValueOf(account))
	if err != nil {
		t.Fatal(err)
	}

	newAccount := &a.Account{}
	reader := bsonrw.NewBSONValueReader(bsontype.EmbeddedDocument, buf.Bytes())
	err = c.DecodeValue(DefaultDecContext, reader, reflect.ValueOf(newAccount))
	t.Fatal(err)
}

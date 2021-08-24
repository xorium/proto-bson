package codec

import (
	a "bitbucket.org/entrlcom/genproto/gen/go/account/v1"
	"bytes"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
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

type AccountTs struct {
	Created *timestamppb.Timestamp
}

func TestAccountTimestampDecoding(t *testing.T) {
	acc := account
	c := NewProtobufMongoCodec()
	buf := make([]byte, 100000000)
	w, err := bsonrw.NewBSONValueWriter(bytes.NewBuffer(buf))
	if err != nil {
		t.Fatal(err)
	}
	err = c.EncodeValue(DefaultEncContext, w, reflect.ValueOf(acc))
	if err != nil {
		t.Fatal(err)
	}
}

package database

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"testing"
	"time"
)

var (
	stubs = []interface{}{
		&Stub{FieldString: "value1", FieldFloat: 10},
		&Stub{FieldString: "value1", FieldFloat: 20},
		&Stub{FieldString: "value1", FieldFloat: 30},
		&Stub{FieldString: "value2", FieldFloat: 10},
		&Stub{FieldString: "value2", FieldFloat: 10},
		&Stub{FieldString: "value3", FieldFloat: 100},
		&Stub{FieldString: "value3", FieldFloat: 20},
		&Stub{FieldString: "value3", FieldFloat: 30},
		&Stub{FieldString: "value4", FieldFloat: 10},
	}
)

type Stub struct {
	FieldString string  `bson:"field_string"`
	FieldFloat  float64 `bson:"field_float"`
}

func TestNewDatabase_Ok(t *testing.T) {
	//Connect to database
	db, err := New([]Option{Dsn("mongodb://localhost:27017/test")}...)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	//Check database availability
	err = db.Ping(nil)
	assert.NoError(t, err)

	//Take collection instance
	collection := db.Collection("test")
	assert.NotNil(t, collection)

	//Drop database
	err = db.Drop()
	assert.NoError(t, err)

	//Close connection to database
	err = db.Close()
	assert.NoError(t, err)
}

func TestNewDatabase_Error(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	opts := [][]Option{
		{Dsn("://localhost:27017/")},
		{Dsn("mongodb://localhost:27017/test"), Mode("unknown")},
		{Dsn("mongodb://localhost:27017/test"), ModeOpts([]readpref.Option{readpref.WithMaxStaleness(100 * time.Microsecond)})},
		{Dsn("mongodb://db:2/test"), Context(ctx)},
	}

	for _, opt := range opts {
		db, err := New(opt...)
		assert.Error(t, err)
		assert.Nil(t, db)
	}
}

func TestPing_ClientIsNil_Error(t *testing.T) {
	db := new(Mongodb)
	err := db.Ping(nil)
	assert.Error(t, err)
	assert.Equal(t, ErrorSessionNotInit, err)
}

func TestClose_ClientIsNil_Error(t *testing.T) {
	db := new(Mongodb)
	err := db.Close()
	assert.Error(t, err)
	assert.Equal(t, ErrorSessionNotInit, err)
}

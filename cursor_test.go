package database

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

type CursorTestSuite struct {
	suite.Suite
	db Database
}

func Test_Cursor(t *testing.T) {
	suite.Run(t, new(CursorTestSuite))
}

func (suite *CursorTestSuite) SetupTest() {
	db, err := New([]Option{Dsn("mongodb://localhost:27017/test")}...)

	if err != nil {
		assert.FailNow(suite.T(), "database init failed", "%v", err)
	}

	res, err := db.Collection("stubs").InsertMany(context.Background(), stubs)

	if err != nil {
		assert.FailNow(suite.T(), "insert stub data to collection failed", "%v", err)
	}

	assert.Len(suite.T(), res.InsertedIDs, len(stubs))

	suite.db = db
}

func (suite *CursorTestSuite) TearDownTest() {
	err := suite.db.Drop()

	if err != nil {
		suite.FailNow("database deletion failed", "%v", err)
	}

	err = suite.db.Close()

	if err != nil {
		suite.FailNow("database closing failed", "%v", err)
	}
}

func (suite *CursorTestSuite) TestCursor_Ok() {
	ctx := context.Background()
	pipeline := []bson.M{
		{
			"$match": bson.M{"field_string": "value1"},
		},
		{
			"$group": bson.M{
				"_id":    "$field_string",
				"amount": bson.M{"$sum": "$field_float"},
			},
		},
	}
	cursor, err := suite.db.Collection("stubs").Aggregate(ctx, pipeline)
	assert.NoError(suite.T(), err)

	var result struct {
		Id     string  `bson:"_id"`
		Amount float64 `bson:"amount"`
	}

	assert.NoError(suite.T(), cursor.Err())
	assert.True(suite.T(), cursor.Next(ctx))
	assert.EqualValues(suite.T(), cursor.ID(), 0)
	err = cursor.Decode(&result)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), result.Id, "value1")
	assert.EqualValues(suite.T(), result.Amount, 60)
	assert.False(suite.T(), cursor.TryNext(ctx))

	err = cursor.Close(ctx)
	assert.NoError(suite.T(), err)
}

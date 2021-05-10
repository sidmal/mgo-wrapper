package database

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

type CollectionTestSuite struct {
	suite.Suite
	db Database
}

func Test_Collection(t *testing.T) {
	suite.Run(t, new(CollectionTestSuite))
}

func (suite *CollectionTestSuite) SetupTest() {
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

func (suite *CollectionTestSuite) TearDownTest() {
	err := suite.db.Drop()

	if err != nil {
		suite.FailNow("database deletion failed", "%v", err)
	}

	err = suite.db.Close()

	if err != nil {
		suite.FailNow("database closing failed", "%v", err)
	}
}

func (suite *CollectionTestSuite) TestCollection_Aggregate_Ok() {
	pipeline := []bson.M{
		{
			"$group": bson.M{
				"_id":    "$field_string",
				"amount": bson.M{"$sum": "$field_float"},
			},
		},
	}
	cursor, err := suite.db.Collection("stubs").Aggregate(context.Background(), pipeline)
	assert.NoError(suite.T(), err)

	var result []struct {
		Id     string  `bson:"_id"`
		Amount float64 `bson:"amount"`
	}
	err = cursor.All(context.Background(), &result)
	assert.Len(suite.T(), result, 4)

	for _, v := range result {
		if v.Id == "value1" {
			assert.EqualValues(suite.T(), v.Amount, 60)
		}

		if v.Id == "value2" {
			assert.EqualValues(suite.T(), v.Amount, 20)
		}

		if v.Id == "value3" {
			assert.EqualValues(suite.T(), v.Amount, 150)
		}

		if v.Id == "value4" {
			assert.EqualValues(suite.T(), v.Amount, 10)
		}
	}

	err = cursor.Close(context.Background())
	assert.NoError(suite.T(), err)
}

func (suite *CollectionTestSuite) TestCollection_Aggregate_Error() {
	pipeline := []bson.M{
		{
			"$unknownFn": bson.M{
				"_id":    "$field_string",
				"amount": bson.M{"$sum": "$field_float"},
			},
		},
	}
	cursor, err := suite.db.Collection("stubs").Aggregate(context.Background(), pipeline)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), cursor)
	tErr, ok := err.(mongo.CommandError)
	assert.True(suite.T(), ok)
	assert.EqualValues(suite.T(), 40324, tErr.Code)
	assert.Regexp(suite.T(), "\\$unknownFn", tErr.Message)
}

func (suite *CollectionTestSuite) TestCollection_CountDocuments_Ok() {
	count, err := suite.db.Collection("stubs").CountDocuments(context.Background(), bson.M{})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), count, len(stubs))
}

func (suite *CollectionTestSuite) TestCollection_DeleteMany_Ok() {
	res, err := suite.db.Collection("stubs").DeleteMany(context.Background(), bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res.DeletedCount, 3)
}

func (suite *CollectionTestSuite) TestCollection_DeleteOne_Ok() {
	ctx := context.Background()

	res, err := suite.db.Collection("stubs").DeleteOne(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res.DeletedCount, 1)

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)

	var result []*Stub
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)
}

func (suite *CollectionTestSuite) TestCollection_Distinct_Ok() {
	res, err := suite.db.Collection("stubs").Distinct(context.Background(), "field_string", bson.M{})
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res, 4)
}

func (suite *CollectionTestSuite) TestCollection_Find_Error() {
	filter := bson.M{
		"field_string": bson.M{
			"$unknownFn": "val",
		},
	}
	cursor, err := suite.db.Collection("stubs").Find(context.Background(), filter)
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), cursor)
	tErr, ok := err.(mongo.CommandError)
	assert.True(suite.T(), ok)
	assert.EqualValues(suite.T(), 2, tErr.Code)
	assert.Regexp(suite.T(), "\\$unknownFn", tErr.Message)
}

func (suite *CollectionTestSuite) TestCollection_FindOne_Ok() {
	var res *Stub
	err := suite.db.Collection("stubs").FindOne(context.Background(), bson.M{"field_string": "value1"}).Decode(&res)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), res)
}

func (suite *CollectionTestSuite) TestCollection_FindOneAndDelete_Ok() {
	ctx := context.Background()

	err := suite.db.Collection("stubs").FindOneAndDelete(ctx, bson.M{"field_string": "value1"}).Err()
	assert.NoError(suite.T(), err)

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)

	var result []*Stub
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)
}

func (suite *CollectionTestSuite) TestCollection_FindOneAndReplace_Ok() {
	newVal := &Stub{
		FieldString: "value5",
		FieldFloat:  111,
	}
	ctx := context.Background()

	err := suite.db.Collection("stubs").FindOneAndReplace(ctx, bson.M{"field_string": "value1"}, newVal).Err()
	assert.NoError(suite.T(), err)

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)

	var result []*Stub
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value5"})
	assert.NoError(suite.T(), err)

	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 1)
}

func (suite *CollectionTestSuite) TestCollection_FindOneAndUpdate_Ok() {
	ctx := context.Background()

	err := suite.db.Collection("stubs").
		FindOneAndUpdate(
			ctx,
			bson.M{"field_string": "value1"},
			bson.M{"$set": bson.M{"field_string": "value6"}},
		).Err()
	assert.NoError(suite.T(), err)

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)

	var result []*Stub
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 2)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value6"})
	assert.NoError(suite.T(), err)

	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 1)
}

func (suite *CollectionTestSuite) TestCollection_InsertOne_Ok() {
	ctx := context.Background()

	err := suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Err()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), mongo.ErrNoDocuments, err)

	doc := &Stub{
		FieldString: "value6",
		FieldFloat:  111,
	}
	res, err := suite.db.Collection("stubs").InsertOne(ctx, doc)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), res.InsertedID)

	var res1 *Stub
	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Decode(&res1)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), res1)
	assert.Equal(suite.T(), doc, res1)
}

func (suite *CollectionTestSuite) TestCollection_ReplaceOne_Ok() {
	ctx := context.Background()
	var res *Stub

	err := suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value4"}).Decode(&res)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), res)
	assert.Equal(suite.T(), res.FieldString, "value4")

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Err()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), mongo.ErrNoDocuments, err)

	doc := &Stub{FieldString: "value6", FieldFloat: 111}
	res1, err := suite.db.Collection("stubs").ReplaceOne(ctx, bson.M{"field_string": "value4"}, doc)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res1.MatchedCount, 1)
	assert.EqualValues(suite.T(), res1.ModifiedCount, 1)

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Decode(&res)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), res)
	assert.Equal(suite.T(), doc, res)

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value4"}).Err()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, mongo.ErrNoDocuments)
}

func (suite *CollectionTestSuite) TestCollection_UpdateMany_Ok() {
	ctx := context.Background()

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)

	var result []*Stub
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 3)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value6"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), result)

	res, err := suite.db.Collection("stubs").UpdateMany(
		ctx,
		bson.M{"field_string": "value1"},
		bson.M{"$set": bson.M{"field_string": "value6"}},
	)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res.MatchedCount, 3)
	assert.EqualValues(suite.T(), res.ModifiedCount, 3)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value6"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), result, 3)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &result)
	assert.NoError(suite.T(), err)
	assert.Empty(suite.T(), result)
}

func (suite *CollectionTestSuite) TestCollection_UpdateOne_Ok() {
	ctx := context.Background()
	var stub *Stub

	err := suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value4"}).Decode(&stub)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stub)

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Err()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, mongo.ErrNoDocuments)

	res, err := suite.db.Collection("stubs").UpdateOne(
		ctx,
		bson.M{"field_string": "value4"},
		bson.M{"$set": bson.M{"field_string": "value6"}},
	)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res.MatchedCount, 1)
	assert.EqualValues(suite.T(), res.ModifiedCount, 1)

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value6"}).Decode(&stub)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), stub)

	err = suite.db.Collection("stubs").FindOne(ctx, bson.M{"field_string": "value4"}).Err()
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), err, mongo.ErrNoDocuments)
}

func (suite *CollectionTestSuite) TestCollection_BulkWrite_Ok() {
	ctx := context.Background()
	models := []mongo.WriteModel{
		mongo.NewUpdateManyModel().
			SetFilter(bson.M{"field_string": "value1"}).
			SetUpdate(bson.M{"$set": bson.M{"field_float": 1}}),
		mongo.NewUpdateManyModel().
			SetFilter(bson.M{"field_string": "value2"}).
			SetUpdate(bson.M{"$set": bson.M{"field_float": 2}}),
	}

	var results []*Stub

	cursor, err := suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &results)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), results)

	for _, v := range results {
		assert.NotEqual(suite.T(), v.FieldFloat, 1)
	}

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value2"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &results)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), results)

	for _, v := range results {
		assert.NotEqual(suite.T(), v.FieldFloat, 2)
	}

	res, err := suite.db.Collection("stubs").BulkWrite(ctx, models)
	assert.NoError(suite.T(), err)
	assert.EqualValues(suite.T(), res.MatchedCount, 5)
	assert.EqualValues(suite.T(), res.ModifiedCount, 5)

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value1"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &results)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), results)

	for _, v := range results {
		assert.EqualValues(suite.T(), v.FieldFloat, 1)
	}

	cursor, err = suite.db.Collection("stubs").Find(ctx, bson.M{"field_string": "value2"})
	assert.NoError(suite.T(), err)
	err = cursor.All(ctx, &results)
	assert.NoError(suite.T(), err)
	assert.NotEmpty(suite.T(), results)

	for _, v := range results {
		assert.EqualValues(suite.T(), v.FieldFloat, 2)
	}
}

func (suite *CollectionTestSuite) TestCollection_Indexes_Ok() {
	res := suite.db.Collection("stubs").Indexes()
	assert.NotNil(suite.T(), res)
}

func (suite *CollectionTestSuite) TestCollection_SingleResult_DecodeBytes_Ok() {
	res := suite.db.Collection("stubs").FindOne(context.Background(), bson.M{"field_string": "value4"})
	assert.NoError(suite.T(), res.Err())

	var firstDecodedResult bson.Raw
	err := res.Decode(&firstDecodedResult)
	assert.NoError(suite.T(), err)
	secondDecodedResult, err := res.DecodeBytes()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), firstDecodedResult, secondDecodedResult)
}

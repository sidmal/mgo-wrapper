# MongoDB database driver wrapper

[![Build Status](https://travis-ci.com/sidmal/mgo-wrapper.svg?branch=main)](https://travis-ci.com/sidmal/mgo-wrapper)
[![codecov](https://codecov.io/gh/sidmal/mgo-wrapper/branch/master/graph/badge.svg)](https://codecov.io/gh/sidmal/mgo-wrapper)

Wrapper to database driver provide next new features:

- Common driver options for setup connection in other projects
- Check DSN url before connect to database
- Caching collection metadata for quick access to database collections
- Database methods mocks for use in tests

## Installation

`go get github.com/sidmal/mgo-wrapper`

## Usage

```go
package main

import (
	"context"
	mgoWrapper "github.com/sidmal/mgo-wrapper"
	"log"
)

func main() {
	opts := []mgoWrapper.Option{
		mgoWrapper.Dsn("mongodb://localhost:27017/db"),
	}
	db, err := mgoWrapper.New(opts...)

	if err != nil {
		log.Fatalln(err)
	}

	data := []interface{}{
		map[string]interface{}{"string": "val 1", "int": 1, "bool": true},
		map[string]interface{}{"string": "val 2", "int": 2, "bool": false},
    }
	_, err = db.Collection("collection").InsertMany(context.Background(), data)

	if err != nil {
		log.Fatalln(err)
	}

	count, err := db.Collection("collection").CountDocuments(context.Background(), nil)

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Found records: %d", count)

	err = db.Close()

	if err != nil {
		log.Fatalln(err)
	}
}
```

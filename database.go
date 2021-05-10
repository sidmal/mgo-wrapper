package database

import (
	"context"
	"errors"
	dsnParser "github.com/sidmal/dsn-parser"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

const (
	DefaultMode           = "primary"
	DefaultContextTimeout = 5 * time.Second
)

var (
	ErrorSessionNotInit = errors.New("database session not init")
)

type Database interface {
	Close() error
	Ping(ctx context.Context) error
	Drop() error
	Collection(name string) CollectionInterface
}

type Mongodb struct {
	name string
	conn *Options
	mx   sync.Mutex

	client      *mongo.Client
	database    *mongo.Database
	collections map[string]*Collection
}

func New(options ...Option) (Database, error) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultContextTimeout)

	opts := Options{}
	conn := &Options{
		Mode:    DefaultMode,
		Context: ctx,
	}

	for _, opt := range options {
		opt(&opts)
	}

	if opts.Dsn != "" {
		conn.Dsn = opts.Dsn
	}

	if opts.Mode != "" {
		conn.Mode = opts.Mode
	}

	if opts.Context != nil {
		conn.Context = opts.Context
	}

	conn.ModeOpts = opts.ModeOpts

	db := new(Mongodb)
	err := db.Open(conn)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func (m *Mongodb) Open(conn *Options) error {
	m.conn = conn
	return m.open()
}

func (m *Mongodb) open() error {
	dsn, err := dsnParser.New(m.conn.Dsn)

	if err != nil {
		return err
	}

	mode, err := readpref.ModeFromString(m.conn.Mode)

	if err != nil {
		return err
	}

	readPref, err := readpref.New(mode, m.conn.ModeOpts...)

	if err != nil {
		return err
	}

	opts := options.Client().
		ApplyURI(m.conn.Dsn).
		SetReadPreference(readPref)
	m.client, err = mongo.Connect(m.conn.Context, opts)

	if err != nil {
		return err
	}

	err = m.client.Ping(m.conn.Context, readPref)

	if err != nil {
		return err
	}

	m.collections = make(map[string]*Collection)
	m.database = m.client.Database(dsn.Database)
	return nil
}

func (m *Mongodb) Close() error {
	if m.client != nil {
		return m.client.Disconnect(m.conn.Context)
	}

	return ErrorSessionNotInit
}

func (m *Mongodb) Ping(ctx context.Context) error {
	if m.client == nil {
		return ErrorSessionNotInit
	}

	if ctx == nil {
		ctx = m.conn.Context
	}

	return m.client.Ping(ctx, readpref.Primary())
}

func (m *Mongodb) Drop() error {
	return m.database.Drop(m.conn.Context)
}

func (m *Mongodb) Collection(name string) CollectionInterface {
	m.mx.Lock()
	col, ok := m.collections[name]

	if !ok {
		col = &Collection{
			collection: m.database.Collection(name),
		}
		m.collections[name] = col
	}
	m.mx.Unlock()
	return col
}

package database

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Options struct {
	Dsn      string
	Mode     string
	ModeOpts []readpref.Option
	Context  context.Context
}

type Option func(*Options)

func Dsn(dsn string) Option {
	return func(opts *Options) {
		opts.Dsn = dsn
	}
}

func Mode(mode string) Option {
	return func(opts *Options) {
		opts.Mode = mode
	}
}

func ModeOpts(val []readpref.Option) Option {
	return func(opts *Options) {
		opts.ModeOpts = val
	}
}

func Context(ctx context.Context) Option {
	return func(opts *Options) {
		opts.Context = ctx
	}
}

package sql2keyval

import (
	"context"
)

type Get func(ctx context.Context, bucket string, key []byte) (val []byte, e error)
type Del func(ctx context.Context, bucket string, key []byte) error
type Add func(ctx context.Context, bucket string, key []byte, val []byte) error
type Set func(ctx context.Context, bucket string, key []byte, val []byte) error

type DelBucket func(ctx context.Context, bucket string) error
type AddBucket func(ctx context.Context, bucket string) error

func NonAtomicSetNew(del Del, add Add) Set {
	return func(ctx context.Context, bucket string, key, val []byte) error {
		_ = del(ctx, bucket, key) // ignore missing key error
		return add(ctx, bucket, key, val)
	}
}

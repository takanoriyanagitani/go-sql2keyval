package sql2keyval

import (
	"context"
)

type Get func(ctx context.Context, bucket string, key []byte) (val []byte, e error)
type Del func(ctx context.Context, bucket string, key []byte) error
type Add func(ctx context.Context, bucket string, key []byte, val []byte) error
type Set func(ctx context.Context, bucket string, key []byte, val []byte) error
type Lst func(ctx context.Context, bucket string, cb func(key []byte) error) error

type DelBucket func(ctx context.Context, bucket string) error
type AddBucket func(ctx context.Context, bucket string) error

type Pair struct {
	Key []byte
	Val []byte
}

type SetMany func(ctx context.Context, bucket string, pairs []Pair) error

type Set2Bucket func(ctx context.Context, key, val []byte) error
type SetMany2Bucket func(ctx context.Context, pairs []Pair) error
type Pairs2Bucket func(ctx context.Context, pairs Iter[Pair]) error

func NonAtomicSetNew(del Del, add Add) Set {
	return func(ctx context.Context, bucket string, key, val []byte) error {
		_ = del(ctx, bucket, key) // ignore missing key error
		return add(ctx, bucket, key, val)
	}
}

func NonAtomicSetsNew(s Set) SetMany {
	return func(ctx context.Context, bucket string, pairs []Pair) error {
		for _, p := range pairs {
			e := s(ctx, bucket, p.Key, p.Val)
			if nil != e {
				return e
			}
		}
		return nil
	}
}

func NonAtomicSetsSingleNew(s Set2Bucket) SetMany2Bucket {
	return func(ctx context.Context, pairs []Pair) error {
		for _, p := range pairs {
			e := s(ctx, p.Key, p.Val)
			if nil != e {
				return e
			}
		}
		return nil
	}
}

func NonAtomicPairs2BucketNew(s Set2Bucket) Pairs2Bucket {
	return func(ctx context.Context, pairs Iter[Pair]) error {
		for o := pairs(); o.HasValue(); o = pairs() {
			p := o.Value()
			e := s(ctx, p.Key, p.Val)
			if nil != e {
				return e
			}
		}
		return nil
	}
}

package sql2keyval

import (
	"context"
	"fmt"
	"sync"
)

var (
	queryGeneratorL sync.RWMutex
	queryGenerators = make(map[string]QueryGenerator)
)

func RegisterQueryGenerator(driverName string, gen QueryGenerator) {
	queryGeneratorL.Lock()
	defer queryGeneratorL.Unlock()
	if nil == gen {
		return // ignore invalid generator
	}

	_, alreadyExists := queryGenerators[driverName]
	if alreadyExists {
		return // use first generator
	}

	queryGenerators[driverName] = gen
}

type Record interface{ Scan(dest ...any) error }

type Query func(ctx context.Context, query string, args ...any) Record
type Exec func(ctx context.Context, query string, args ...any) error

type QueryGenerator interface {
	Get(bucket string) (query string, e error)
	Del(bucket string) (query string, e error)
	Add(bucket string) (query string, e error)
	Set(bucket string) (query string, e error)
	DelBucket(bucket string) (query string, e error)
	AddBucket(bucket string) (query string, e error)
}

type emptyQueryGenerator struct{ err error }

func (e *emptyQueryGenerator) Get(_ string) (string, error)       { return "", e.err }
func (e *emptyQueryGenerator) Del(_ string) (string, error)       { return "", e.err }
func (e *emptyQueryGenerator) Add(_ string) (string, error)       { return "", e.err }
func (e *emptyQueryGenerator) Set(_ string) (string, error)       { return "", e.err }
func (e *emptyQueryGenerator) DelBucket(_ string) (string, error) { return "", e.err }
func (e *emptyQueryGenerator) AddBucket(_ string) (string, error) { return "", e.err }

func record2val(r Record) (v []byte, e error) {
	e = r.Scan(v)
	return
}

func getterNew(g QueryGenerator, q Query) Get {
	return func(ctx context.Context, bucket string, key []byte) (val []byte, e error) {
		query, e := g.Get(bucket)
		if nil != e {
			return nil, e
		}
		record := q(ctx, query, key)
		return record2val(record)
	}
}

func adderNew(g QueryGenerator, q Exec) Add {
	return func(ctx context.Context, bucket string, key, val []byte) error {
		query, e := g.Add(bucket)
		if nil != e {
			return e
		}
		return q(ctx, query, key, val)
	}
}

func setterNew(g QueryGenerator, q Exec) Set {
	return func(ctx context.Context, bucket string, key, val []byte) error {
		query, e := g.Set(bucket)
		if nil != e {
			return e
		}
		return q(ctx, query, key, val)
	}
}

func removerNew(g QueryGenerator, q Exec) Del {
	return func(ctx context.Context, bucket string, key []byte) error {
		query, e := g.Del(bucket)
		if nil != e {
			return e
		}
		return q(ctx, query, key)
	}
}

func delBucketNew(g QueryGenerator, q Exec) DelBucket {
	return func(ctx context.Context, bucket string) error {
		query, e := g.DelBucket(bucket)
		if nil != e {
			return e
		}
		return q(ctx, query)
	}
}

func addBucketNew(g QueryGenerator, q Exec) AddBucket {
	return func(ctx context.Context, bucket string) error {
		query, e := g.AddBucket(bucket)
		if nil != e {
			return e
		}
		return q(ctx, query)
	}
}

func curry[T, U, V any](f func(T, U) V) func(T) func(U) V {
	return func(t T) func(U) V {
		return func(u U) V {
			return f(t, u)
		}
	}
}

func compose[T, U, V any](f func(T) U, g func(U) V) func(T) V {
	return func(t T) V {
		u := f(t)
		return g(u)
	}
}

var getFactory func(QueryGenerator) func(Query) Get = curry(getterNew)
var addFactory func(QueryGenerator) func(Exec) Add = curry(adderNew)
var setFactory func(QueryGenerator) func(Exec) Set = curry(setterNew)
var delFactory func(QueryGenerator) func(Exec) Del = curry(removerNew)
var delBucketFactory func(QueryGenerator) func(Exec) DelBucket = curry(delBucketNew)
var addBucketFactory func(QueryGenerator) func(Exec) AddBucket = curry(addBucketNew)

func getQueryGenerator(driverName string) (QueryGenerator, error) {
	queryGeneratorL.RLock()
	gen, found := queryGenerators[driverName]
	queryGeneratorL.RUnlock()
	if found {
		return gen, nil
	}
	return nil, fmt.Errorf("No query generator found: %s", driverName)
}

func getQueryGeneratorOrEmpty(driverName string) QueryGenerator {
	q, e := getQueryGenerator(driverName)
	if nil != e {
		return &emptyQueryGenerator{err: fmt.Errorf("No generator found: %s", driverName)}
	}
	return q
}

var GetFactory func(driverName string) func(Query) Get = compose(getQueryGeneratorOrEmpty, getFactory)
var AddFactory func(driverName string) func(Exec) Add = compose(getQueryGeneratorOrEmpty, addFactory)
var SetFactory func(driverName string) func(Exec) Set = compose(getQueryGeneratorOrEmpty, setFactory)
var DelFactory func(driverName string) func(Exec) Del = compose(getQueryGeneratorOrEmpty, delFactory)
var DelBucketFactory func(driverName string) func(Exec) DelBucket = compose(getQueryGeneratorOrEmpty, delBucketFactory)
var AddBucketFactory func(driverName string) func(Exec) AddBucket = compose(getQueryGeneratorOrEmpty, addBucketFactory)

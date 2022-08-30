package sql2keyval

import (
	"context"
	"fmt"
	"testing"
)

func TestNonAtomicSetNew(t *testing.T) {
	t.Parallel()

	var dummyRemover Del = func(_c context.Context, _b string, _k []byte) error {
		return fmt.Errorf("must fail")
	}

	var dummyAdder Add = func(_c context.Context, _b string, _k []byte, _v []byte) error {
		return fmt.Errorf("must fail")
	}

	var dummySetter Set = NonAtomicSetNew(dummyRemover, dummyAdder)
	e := dummySetter(context.Background(), "", nil, nil)
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestNonAtomicSetsNew(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		var dummySetter Set = func(_c context.Context, _b string, _k []byte, _v []byte) error {
			return fmt.Errorf("Must fail")
		}

		var dummySetMany SetMany = NonAtomicSetsNew(dummySetter)

		e := dummySetMany(context.Background(), "", []Pair{
			{Key: []byte("k"), Val: []byte("v")},
		})
		if nil == e {
			t.Errorf("Must fail")
		}
	})

	t.Run("no error", func(t *testing.T) {
		t.Parallel()
		var dummySetter Set = func(_c context.Context, _b string, _k []byte, _v []byte) error {
			return nil
		}

		var dummySetMany SetMany = NonAtomicSetsNew(dummySetter)

		e := dummySetMany(context.Background(), "", []Pair{
			{Key: []byte("k"), Val: []byte("v")},
		})
		if nil != e {
			t.Errorf("Must not fail")
		}
	})
}

func TestSql2KeyVal(t *testing.T) {
	t.Parallel()

	var ngSetter Set2Bucket = func(_ctx context.Context, key, val []byte) error {
		return fmt.Errorf("Must fail")
	}

	var okSetter Set2Bucket = func(_ctx context.Context, key, val []byte) error {
		return nil
	}

	t.Run("NonAtomicSetsSingleNew", func(t *testing.T) {
		t.Parallel()

		t.Run("ng", func(t *testing.T) {
			t.Parallel()
			var sm2 SetMany2Bucket = NonAtomicSetsSingleNew(ngSetter)
			e := sm2(context.Background(), []Pair{
				{Key: nil, Val: nil},
			})
			if nil == e {
				t.Errorf("Must fail")
			}
		})

		t.Run("ok,empty", func(t *testing.T) {
			t.Parallel()
			var sm2 SetMany2Bucket = NonAtomicSetsSingleNew(okSetter)
			e := sm2(context.Background(), nil)
			if nil != e {
				t.Errorf("Must not fail: %v", e)
			}
		})

		t.Run("ok,non empty", func(t *testing.T) {
			t.Parallel()
			var sm2 SetMany2Bucket = NonAtomicSetsSingleNew(okSetter)
			e := sm2(context.Background(), []Pair{
				{Key: nil, Val: nil},
			})
			if nil != e {
				t.Errorf("Must not fail: %v", e)
			}
		})
	})

	t.Run("NonAtomicPairs2BucketNew", func(t *testing.T) {
		t.Parallel()

		t.Run("ok,empty", func(t *testing.T) {
			t.Parallel()
			var p2b Pairs2Bucket = NonAtomicPairs2BucketNew(okSetter)
			e := p2b(context.Background(), IterEmptyNew[Pair]())
			if nil != e {
				t.Errorf("Must not fail: %v", e)
			}
		})

		t.Run("ok,non empty", func(t *testing.T) {
			t.Parallel()
			var p2b Pairs2Bucket = NonAtomicPairs2BucketNew(okSetter)
			e := p2b(context.Background(), IterFromArray([]Pair{
				{Key: nil, Val: nil},
			}))
			if nil != e {
				t.Errorf("Must not fail: %v", e)
			}
		})

		t.Run("ng,non empty", func(t *testing.T) {
			t.Parallel()
			var p2b Pairs2Bucket = NonAtomicPairs2BucketNew(ngSetter)
			e := p2b(context.Background(), IterFromArray([]Pair{
				{Key: nil, Val: nil},
			}))
			if nil == e {
				t.Errorf("Must fail")
			}
		})
	})
}

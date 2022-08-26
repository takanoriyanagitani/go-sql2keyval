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

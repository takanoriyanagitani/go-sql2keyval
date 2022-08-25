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

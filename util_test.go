package sql2keyval

import (
	"fmt"
	"testing"
)

func TestBool2error(t *testing.T) {
	t.Parallel()

	t.Run("no error", func(t *testing.T) {
		t.Parallel()
		e := Bool2error(true, func() error { return fmt.Errorf("must not be error") })
		if nil != e {
			t.Errorf("Error must be nil")
		}

		e = Bool2error(true, nil)
		if nil != e {
			t.Errorf("Error must be nil")
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		e := Bool2error(false, func() error { return fmt.Errorf("must be error") })
		if nil == e {
			t.Errorf("Error must be non nil")
		}
	})
}

func TestCompose(t *testing.T) {
	t.Parallel()
	var s2len func(string) int = func(s string) int { return len(s) }
	var double func(i int) (doubled int) = func(i int) int { return 2 * i }
	var doubleStringLen func(string) int = Compose(s2len, double)
	doubledLen := doubleStringLen("0123456789abcdefghijk")
	if 42 != doubledLen {
		t.Errorf("Unexpected result: %v", doubledLen)
	}
}

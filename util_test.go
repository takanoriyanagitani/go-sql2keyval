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

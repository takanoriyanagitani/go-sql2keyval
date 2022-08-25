package stdsql

import (
	"testing"
	"database/sql"
)

func TestDbOpenNew(t *testing.T) {
	t.Parallel()
	var newOpener func(string)(*sql.DB, error) = DbOpenNew("does-not-exist")
	_, e := newOpener("")
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestQueryNew(t *testing.T) {
	t.Parallel()
	QueryNew(nil)
}

func TestExecNew(t *testing.T) {
	t.Parallel()
	ExecNew(nil)
}

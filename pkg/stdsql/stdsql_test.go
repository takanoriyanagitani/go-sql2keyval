package stdsql

import (
	"database/sql"
	"testing"
)

func TestDbOpenNew(t *testing.T) {
	t.Parallel()
	var newOpener func(string) (*sql.DB, error) = DbOpenNew("does-not-exist")
	_, e := newOpener("")
	if nil == e {
		t.Errorf("Must fail")
	}
}

func TestQueryNew(t *testing.T) {
	t.Parallel()
	QueryNew(nil)
}

func TestQueryCbNew(t *testing.T) {
	t.Parallel()
	QueryCbNew(nil)
}

func TestExecNew(t *testing.T) {
	t.Parallel()
	ExecNew(nil)
}

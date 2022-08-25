package pg

import (
	"strings"
	"testing"
)

func TestNewQueryGeneratorMust(t *testing.T) {
	t.Parallel()

	t.Run("no panic", func(t *testing.T) {
		t.Parallel()
		newQueryGeneratorMust()
	})

	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		t.Run("empty bucket", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			_, e := qgen.Get("")
			if nil == e {
				t.Errorf("Must reject empty tablename")
			}
		})

		t.Run("first char is number", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			_, e := qgen.Get("0zero")
			if nil == e {
				t.Errorf("Must reject invalid prefix")
			}
		})

		t.Run("too long tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			_, e := qgen.Get("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopqrstuv")
			if nil == e {
				t.Errorf("Must reject too long tablename")
			}
		})

		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.Get("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				SELECT val FROM t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq
				WHERE key=$1
				LIMIT 1
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Parallel()
		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.Set("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				INSERT INTO t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq AS alias_insert (key, val)
				VALUES ($1, $2)
				ON CONFLICT ON CONSTRAINT t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq_pkc
				DO UPDATE SET val=EXCLUDED.val
				WHERE alias_insert.val != EXCLUDED.val
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})

	t.Run("Del", func(t *testing.T) {
		t.Parallel()
		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.Del("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				DELETE FROM t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq
				WHERE key=$1
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})

	t.Run("Add", func(t *testing.T) {
		t.Parallel()
		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.Add("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				INSERT INTO t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq(key, val)
				VALUES ($1, $2)
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})

	t.Run("BDel", func(t *testing.T) {
		t.Parallel()
		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.DelBucket("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				DROP TABLE IF EXISTS t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})

	t.Run("BAdd", func(t *testing.T) {
		t.Parallel()
		t.Run("'short' tablename", func(t *testing.T) {
		    t.Parallel()
			qgen := newQueryGeneratorMust()
			query, e := qgen.AddBucket("t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq")
			if nil != e {
				t.Errorf("Must accept 'short' tablename")
			}
			expected := `
				CREATE TABLE IF NOT EXISTS t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq(
				  key BYTEA,
				  val BYTEA NOT NULL,
				  CONSTRAINT t123456789abcdefghijklmnopqrstuv0123456789abcdefghijklmnopq_pkc PRIMARY KEY(key)
				)
			`
			tq := strings.ReplaceAll(strings.TrimSpace(query), "	", "")
			te := strings.ReplaceAll(strings.TrimSpace(expected), "	", "")
			if tq != te {
				t.Errorf("Unexpected value.\n")
				t.Errorf("Expected: %s\n", te)
				t.Errorf("Got: %s\n", tq)
			}
		})
	})
}

package pgx2kv

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func TestAll(t *testing.T) {
	t.Parallel()

	pgx_dbname := os.Getenv("ITEST_SQL2KEYVAL_PGX_DBNAME")
	if len(pgx_dbname) < 1 {
		t.Skip("skipping pgx test...")
	}

	p, e := pgxpool.Connect(context.Background(), "dbname="+pgx_dbname)
	if nil != e {
		t.Errorf("Unable to connect to test db: %v", e)
	}

	t.Run("PgxBulkSetNew", func(t *testing.T) {
		t.Parallel()
		var sm s2k.SetMany = PgxBulkSetNew(p)
		var ab s2k.AddBucket = PgxAddBucketNew(p)

		t.Run("empty", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), "", nil)
			if nil != e {
				t.Errorf("Should be nop: %v", e)
			}
		})

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), "0table", []s2k.Pair{
				{Key: []byte("k"), Val: []byte("v")},
			})
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			t.Run("add bucket", func(t *testing.T) {
				e := ab(context.Background(), "test_bulkset")
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})
		})
	})

	t.Cleanup(func() {
		p.Close()
	})
}

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
			tname := "test_bulkset"
			t.Run("add bucket", func(t *testing.T) {
				e := ab(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})

			t.Run("invalid key", func(t *testing.T) {
				e := sm(context.Background(), tname, []s2k.Pair{
					{Key: nil, Val: []byte("v")},
				})
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})

			t.Run("valid key", func(t *testing.T) {
				e := sm(context.Background(), tname, []s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
				})
				if nil != e {
					t.Errorf("Unable to set valid key/val: %v", e)
				}
			})

			t.Run("partial invalid key", func(t *testing.T) {
				e := sm(context.Background(), tname, []s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
					{Key: nil, Val: []byte("v")},
					{Key: []byte("l"), Val: []byte("v")},
				})
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})
		})
	})

	t.Run("PgxBatchUpsertNew", func(t *testing.T) {
		t.Parallel()
		var sm s2k.SetBatch = PgxBatchUpsertNew(p)
		var ab s2k.AddBucket = PgxAddBucketNew(p)

		t.Run("empty", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), s2k.IterEmptyNew[s2k.Batch]())
			if nil != e {
				t.Errorf("Should be nop: %v", e)
			}
		})

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), s2k.IterFromArray([]s2k.Batch{
				s2k.BatchNew("0table", nil, nil),
			}))
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			tname := "test_batch_upsert"

			tnames := []string{
				tname + "_1",
				tname + "_2",
				tname + "_3",
			}

			t.Run("add buckets", func(t *testing.T) {
				for _, tn := range tnames {
					e := ab(context.Background(), tn)
					if nil != e {
						t.Errorf("Unable to create table: %v", e)
					}
				}
			})

			t.Run("upserts", func(t *testing.T) {
				e := sm(context.Background(), s2k.IterFromArray([]s2k.Batch{
					s2k.BatchNew(tname+"_1", []byte("k"), []byte("v")),
					s2k.BatchNew(tname+"_2", []byte("k"), []byte("v")),
					s2k.BatchNew(tname+"_3", []byte("k"), []byte("v")),
				}))
				if nil != e {
					t.Errorf("Unable to upsert: %v", e)
				}
			})
		})
	})

	t.Run("PgxAddBucketNew", func(t *testing.T) {
		t.Parallel()
		var ab s2k.AddBucket = PgxAddBucketNew(p)

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			e := ab(context.Background(), "0table")
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			tname := "test_addbucket_new_pgx"
			t.Run("add table", func(t *testing.T) {
				e := ab(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})

			t.Run("check table", func(t *testing.T) {
				row := p.QueryRow(context.Background(), `
					SELECT COUNT(*) AS cnt FROM pg_class
					WHERE
					  relname = $1::TEXT
					AND relkind = 'r'
			    `, tname)

				var cnt int64
				e := row.Scan(&cnt)
				if nil != e {
					t.Errorf("Unable to get table name: %v", e)
				}

				if 1 != cnt {
					t.Errorf("Unexpected number of tables: %v", cnt)
				}
			})

		})
	})

	t.Run("PgxDelBucketNew", func(t *testing.T) {
		t.Parallel()
		var ab s2k.AddBucket = PgxAddBucketNew(p)
		var db s2k.DelBucket = PgxDelBucketNew(p)

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			e := ab(context.Background(), "0table")
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			tname := "test_delbucket_new_pgx"
			t.Run("add table", func(t *testing.T) {
				e := ab(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})

			t.Run("del table", func(t *testing.T) {
				e := db(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to drop table: %v", e)
				}
			})

			t.Run("check table", func(t *testing.T) {
				row := p.QueryRow(context.Background(), `
					SELECT COUNT(*) AS cnt FROM pg_class
					WHERE
					  relname = $1::TEXT
					AND relkind = 'r'
			    `, tname)

				var cnt int64
				e := row.Scan(&cnt)
				if nil != e {
					t.Errorf("Unable to get table name: %v", e)
				}

				if 0 != cnt {
					t.Errorf("Unexpected number of tables: %v", cnt)
				}
			})

		})
	})

	t.Run("PgxBulkSetSingleBuilder", func(t *testing.T) {
		t.Parallel()

		tname := "test_upserts2single"
		var sm s2k.SetMany2Bucket = PgxBulkSetSingleBuilder(tname)(p)
		var ab s2k.AddBucket = PgxAddBucketNew(p)

		t.Run("empty", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), nil)
			if nil != e {
				t.Errorf("Should be nop: %v", e)
			}
		})

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			var smInvalid s2k.SetMany2Bucket = PgxBulkSetSingleBuilder("0invl")(p)
			e := smInvalid(context.Background(), []s2k.Pair{
				{Key: []byte("k"), Val: []byte("v")},
			})
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			t.Run("add bucket", func(t *testing.T) {
				e := ab(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})

			t.Run("invalid key", func(t *testing.T) {
				e := sm(context.Background(), []s2k.Pair{
					{Key: nil, Val: []byte("v")},
				})
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})

			t.Run("valid key", func(t *testing.T) {
				e := sm(context.Background(), []s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
				})
				if nil != e {
					t.Errorf("Unable to set valid key/val: %v", e)
				}
			})

			t.Run("partial invalid key", func(t *testing.T) {
				e := sm(context.Background(), []s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
					{Key: nil, Val: []byte("v")},
					{Key: []byte("l"), Val: []byte("v")},
				})
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})
		})
	})

	t.Run("PgxPairs2BucketSingleBuilder", func(t *testing.T) {
		t.Parallel()

		tname := "test_upserts2single_iter"
		var sm s2k.Pairs2Bucket = PgxPairs2BucketSingleBuilder(tname)(p)
		var ab s2k.AddBucket = PgxAddBucketNew(p)

		t.Run("empty", func(t *testing.T) {
			t.Parallel()
			e := sm(context.Background(), s2k.IterEmptyNew[s2k.Pair]())
			if nil != e {
				t.Errorf("Should be nop: %v", e)
			}
		})

		t.Run("invalid table name", func(t *testing.T) {
			t.Parallel()
			var smInvalid s2k.Pairs2Bucket = PgxPairs2BucketSingleBuilder("0invl")(p)
			e := smInvalid(context.Background(), s2k.IterFromArray([]s2k.Pair{
				{Key: []byte("k"), Val: []byte("v")},
			}))
			if nil == e {
				t.Errorf("Must reject invalid table name")
			}
		})

		// non parallel
		t.Run("ordered", func(t *testing.T) {
			t.Run("add bucket", func(t *testing.T) {
				e := ab(context.Background(), tname)
				if nil != e {
					t.Errorf("Unable to create table: %v", e)
				}
			})

			t.Run("invalid key", func(t *testing.T) {
				e := sm(context.Background(), s2k.IterFromArray([]s2k.Pair{
					{Key: nil, Val: []byte("v")},
				}))
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})

			t.Run("valid key", func(t *testing.T) {
				e := sm(context.Background(), s2k.IterFromArray([]s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
				}))
				if nil != e {
					t.Errorf("Unable to set valid key/val: %v", e)
				}
			})

			t.Run("partial invalid key", func(t *testing.T) {
				e := sm(context.Background(), s2k.IterFromArray([]s2k.Pair{
					{Key: []byte("k"), Val: []byte("v")},
					{Key: nil, Val: []byte("v")},
					{Key: []byte("l"), Val: []byte("v")},
				}))
				if nil == e {
					t.Errorf("Must reject invalid key")
				}
			})
		})
	})

	t.Cleanup(func() {
		p.Close()
	})
}

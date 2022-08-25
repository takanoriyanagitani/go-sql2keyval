package pgx_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	// use pgx driver to connect to postgres
	_ "github.com/jackc/pgx/v4/stdlib"

	// use postgres driver to generate sql
	_ "github.com/takanoriyanagitani/go-sql2keyval/pkg/sqldb/postgres"

	sk "github.com/takanoriyanagitani/go-sql2keyval"
	ss "github.com/takanoriyanagitani/go-sql2keyval/pkg/stdsql"
)

var (
	testDb *sql.DB
)

func init() {
	dbname, e := getPgxEnvDB()
	if nil != e {
		// unit test only
		// no need to set up *sql.DB
		return
	}

	dbnew := ss.DbOpenNew("pgx")
	conn := strings.Join([]string{
		"dbname=", dbname,
	}, " ")

	testDb, e = dbnew(conn)
	if nil != e {
		panic(e)
	}
}

func getPgxEnvDB() (string, error) {
	dbname4pgxTest := os.Getenv("ITEST_SQL2KEYVAL_PGX_DBNAME")
	if 0 < len(dbname4pgxTest) {
		return dbname4pgxTest, nil
	}
	return "", fmt.Errorf("skipping pgx test")
}

func getRowNew(d *sql.DB) func(query string, args ...any) *sql.Row { return d.QueryRow }

func TestAll(t *testing.T) {
	if nil == testDb {
		t.Skip("skipping pgx test")
	}

	var rowGetter func(query string, args ...any) *sql.Row = getRowNew(testDb)

	t.Parallel()

	t.Run("QueryNew", func(t *testing.T) {
		t.Parallel()
		var q sk.Query = ss.QueryNew(testDb)
		if nil == q {
			t.Errorf("Unable to get query interface")
		}
	})

	t.Run("ExecNew", func(t *testing.T) {
		t.Parallel()
		var q sk.Exec = ss.ExecNew(testDb)
		if nil == q {
			t.Errorf("Unable to get exec interface")
		}
	})

	t.Run("Del/AddBucketFactory", func(t *testing.T) {
		t.Parallel() // sub tests: non parallel

		t.Run("add", func(t *testing.T) {
			var newBucketAdder func(sk.Exec) sk.AddBucket = sk.AddBucketFactory("postgres")
			if nil == newBucketAdder {
				t.Errorf("Unable to get add bucket factory")
			}
			var exec sk.Exec = ss.ExecNew(testDb)
			var newBucket sk.AddBucket = newBucketAdder(exec)
			if nil == newBucket {
				t.Errorf("Unable to get bucket creator")
			}

			tablename := "bid_cafef00d_dead_beaf_face_864299792458_ymd_2022_08_25"
			e := newBucket(context.Background(), tablename)
			if nil != e {
				t.Errorf("Unable to create bucket: %v", e)
			}

			var tableCount *sql.Row = rowGetter(
				`
		      SELECT COUNT(*) AS tcnt FROM pg_class
		  	WHERE
		  	  relname=$1
		  	  AND relkind='r'
		    `,
				tablename,
			)

			var tcnt int64
			e = tableCount.Scan(&tcnt)
			if nil != e {
				t.Errorf("Unable to get table count: %v", e)
			}

			if 1 != tcnt {
				t.Errorf("Bucket not found. tablecount: %v", tcnt)
			}
		})

		t.Run("del", func(t *testing.T) {
			var newBucketRemover func(sk.Exec) sk.DelBucket = sk.DelBucketFactory("postgres")
			if nil == newBucketRemover {
				t.Errorf("Unable to get del bucket factory")
			}
			var exec sk.Exec = ss.ExecNew(testDb)
			var delBucket sk.DelBucket = newBucketRemover(exec)
			if nil == delBucket {
				t.Errorf("Unable to get bucket remover")
			}

			tablename := "bid_cafef00d_dead_beaf_face_864299792458_ymd_2022_08_25"
			e := delBucket(context.Background(), tablename)
			if nil != e {
				t.Errorf("Unable to remove bucket: %v", e)
			}

			var tableCount *sql.Row = rowGetter(
				`
					SELECT COUNT(*) AS tcnt FROM pg_class
					WHERE
					relname=$1
					AND relkind='r'
		    	`,
				tablename,
			)

			var tcnt int64
			e = tableCount.Scan(&tcnt)
			if nil != e {
				t.Errorf("Unable to get table count: %v", e)
			}

			if 0 != tcnt {
				t.Errorf("Bucket found. tablecount: %v", tcnt)
			}
		})
	})

	t.Run("Set/Del", func(t *testing.T) {
		t.Parallel() // sub tests: non parallel

		var exec sk.Exec = ss.ExecNew(testDb)

		var newBucketAdder func(sk.Exec) sk.AddBucket = sk.AddBucketFactory("postgres")
		if nil == newBucketAdder {
			t.Errorf("Unable to get add bucket factory")
		}
		var newBucket sk.AddBucket = newBucketAdder(exec)
		if nil == newBucket {
			t.Errorf("Unable to get bucket creator")
		}

		tablename := "testadd_cafef00d_dead_beaf_face_864299792458_ymd_2022_08_25"
		e := newBucket(context.Background(), tablename)
		if nil != e {
			t.Errorf("Unable to create bucket: %v", e)
		}

		t.Run("set", func(t *testing.T) {
			var newSetter func(sk.Exec) sk.Set = sk.SetFactory("postgres")
			if nil == newSetter {
				t.Errorf("Unable to get setter factory")
			}

			var setter sk.Set = newSetter(exec)
			if nil == setter {
				t.Errorf("Unable to get setter")
			}

			key := []byte("14:18:07")
			val := []byte("hw")

			e := setter(context.Background(), tablename, key, val)
			if nil != e {
				t.Errorf("Unable to set key/value: %v", e)
			}

			row := rowGetter(
				`
					SELECT val FROM testadd_cafef00d_dead_beaf_face_864299792458_ymd_2022_08_25
					WHERE key=$1
			    `,
				key,
			)

			var got_val []byte
			e = row.Scan(&got_val)
			if nil != e {
				t.Errorf("Unable to get value: %v", e)
			}

			if 0 != bytes.Compare(got_val, val) {
				t.Errorf("Unexpected value got.\n")
			}
		})
	})

	t.Cleanup(func() {
		testDb.Close()
	})
}

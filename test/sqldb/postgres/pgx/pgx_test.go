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

	t.Run("QueryCbNew", func(t *testing.T) {
		t.Parallel()
		var q sk.QueryCb = ss.QueryCbNew(testDb)
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

	t.Run("Set/Add/Del/Get", func(t *testing.T) {
		t.Parallel() // sub tests: non parallel

		var exec sk.Exec = ss.ExecNew(testDb)
		var qry sk.Query = ss.QueryNew(testDb)

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

		t.Run("set(1st)", func(t *testing.T) {
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

		t.Run("set(2nd)", func(t *testing.T) {
			var newSetter func(sk.Exec) sk.Set = sk.SetFactory("postgres")
			if nil == newSetter {
				t.Errorf("Unable to get setter factory")
			}

			var setter sk.Set = newSetter(exec)
			if nil == setter {
				t.Errorf("Unable to get setter")
			}

			key := []byte("14:18:07")
			val := []byte("hh")

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

		t.Run("add(before del)", func(t *testing.T) {
			var newAdder func(sk.Exec) sk.Add = sk.AddFactory("postgres")
			if nil == newAdder {
				t.Errorf("Unable to get adder factory")
			}

			var adder sk.Add = newAdder(exec)
			if nil == adder {
				t.Errorf("Unable to get adder")
			}

			key := []byte("14:18:07")
			val := []byte("hh")

			e := adder(context.Background(), tablename, key, val)
			if nil == e {
				t.Errorf("Must be rejected(dup key)")
			}
		})

		t.Run("del", func(t *testing.T) {
			var newRemover func(sk.Exec) sk.Del = sk.DelFactory("postgres")
			if nil == newRemover {
				t.Errorf("Unable to get remover factory")
			}

			var remover sk.Del = newRemover(exec)
			if nil == remover {
				t.Errorf("Unable to get remover")
			}

			key := []byte("14:18:07")

			e := remover(context.Background(), tablename, key)
			if nil != e {
				t.Errorf("Unable to remove key: %v", e)
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
			if nil == e {
				t.Errorf("Must be ErrNoRows")
			}
		})

		t.Run("get(before add)", func(t *testing.T) {
			var newGetter func(sk.Query) sk.Get = sk.GetFactory("postgres")
			if nil == newGetter {
				t.Errorf("Unable to get getter factory")
			}

			var getter sk.Get = newGetter(qry)
			if nil == getter {
				t.Errorf("Unable to get getter")
			}

			key := []byte("14:18:07")

			_, e := getter(context.Background(), tablename, key)
			if nil == e {
				t.Errorf("Must be error")
			}
		})

		t.Run("add(after del)", func(t *testing.T) {
			var newAdder func(sk.Exec) sk.Add = sk.AddFactory("postgres")
			if nil == newAdder {
				t.Errorf("Unable to get adder factory")
			}

			var adder sk.Add = newAdder(exec)
			if nil == adder {
				t.Errorf("Unable to get adder")
			}

			key := []byte("14:18:07")
			val := []byte("hh")

			e := adder(context.Background(), tablename, key, val)
			if nil != e {
				t.Errorf("Unable to add key/value: %v", e)
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

		t.Run("get(after add)", func(t *testing.T) {
			var newGetter func(sk.Query) sk.Get = sk.GetFactory("postgres")
			if nil == newGetter {
				t.Errorf("Unable to get getter factory")
			}

			var getter sk.Get = newGetter(qry)
			if nil == getter {
				t.Errorf("Unable to get getter")
			}

			key := []byte("14:18:07")
			val := []byte("hh")

			got, e := getter(context.Background(), tablename, key)
			if nil != e {
				t.Errorf("Unable to get: %v", e)
			}

			if 0 != bytes.Compare(got, val) {
				t.Errorf("Unexpected value got.")
			}
		})
	})

	t.Run("Set/Lst", func(t *testing.T) {
		t.Parallel() // sub tests: non parallel

		var exec sk.Exec = ss.ExecNew(testDb)
		var qry sk.QueryCb = ss.QueryCbNew(testDb)

		var newBucketAdder func(sk.Exec) sk.AddBucket = sk.AddBucketFactory("postgres")
		if nil == newBucketAdder {
			t.Errorf("Unable to get add bucket factory")
		}
		var newBucket sk.AddBucket = newBucketAdder(exec)
		if nil == newBucket {
			t.Errorf("Unable to get bucket creator")
		}

		tablename := "testlst_cafef00d_dead_beaf_face_864299792458_ymd_2022_08_25"
		e := newBucket(context.Background(), tablename)
		if nil != e {
			t.Errorf("Unable to create bucket: %v", e)
		}

		pairs := []sk.Pair{
			{Key: []byte("14:18:07"), Val: []byte("hw")},
			{Key: []byte("14:18:08"), Val: []byte("hh")},
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

			var setMany sk.SetMany = sk.NonAtomicSetsNew(setter)

			e := setMany(context.Background(), tablename, pairs)

			if nil != e {
				t.Errorf("Unable to set key/value: %v", e)
			}
		})

		t.Run("lst(after set)", func(t *testing.T) {
			var newLister func(sk.QueryCb) sk.Lst = sk.LstFactory("postgres")
			if nil == newLister {
				t.Errorf("Unable to get list factory")
			}

			var lister sk.Lst = newLister(qry)
			if nil == lister {
				t.Errorf("Unable to get lister")
			}

			var keys [][]byte

			e := lister(context.Background(), tablename, func(key []byte) error {
				keys = append(keys, key)
				return nil
			})
			if nil != e {
				t.Errorf("Unable to get: %v", e)
			}

			if 2 != len(keys) {
				t.Errorf("Unexpected number of keys: %v", len(keys))
			}

			if 0 != bytes.Compare(keys[0], pairs[0].Key) {
				t.Errorf("Unexpected key")
			}

			if 0 != bytes.Compare(keys[1], pairs[1].Key) {
				t.Errorf("Unexpected key")
			}
		})
	})

	t.Cleanup(func() {
		testDb.Close()
	})
}

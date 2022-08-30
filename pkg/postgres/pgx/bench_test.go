package pgx2kv

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func BenchmarkSetMany(b *testing.B) {
	pgx_dbname := os.Getenv("ITEST_SQL2KEYVAL_PGX_DBNAME")
	if len(pgx_dbname) < 1 {
		b.Skip("skipping pgx test...")
	}

	p, e := pgxpool.Connect(context.Background(), "dbname="+pgx_dbname)
	if nil != e {
		b.Errorf("Unable to get pgx pool: %v", e)
	}

	var ab s2k.AddBucket = PgxAddBucketNew(p)
	var sm s2k.SetMany = PgxBulkSetNew(p)

	tname := "benchmark_setmany"

	e = ab(context.Background(), tname)
	if nil != e {
		b.Errorf("Unable to create test table: %v", e)
	}

	b.Run("BenchmarkAll", func(b *testing.B) {
		b.Run("many bucket, many key/value", func(b *testing.B) {
			b.Run("empty pair", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sm(context.Background(), tname, nil)
						if nil != e {
							b.Errorf("Should be nop")
						}
					}
				})
			})

			b.Run("single pair", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var i uint64 = 0
					buf8 := make([]byte, 8)
					pairs := []s2k.Pair{
						{
							Key: nil,
							Val: nil,
						},
					}
					for pb.Next() {
						binary.LittleEndian.PutUint64(buf8, i)
						pairs[0].Key = buf8
						e := sm(context.Background(), tname, pairs)
						if nil != e {
							b.Errorf("Unable to add key/val: %v", e)
						}
						i += 1
						b.SetBytes(8)
					}
				})
			})
		})

		b.Run("single bucket, many key/value", func(b *testing.B) {
			var sb s2k.SetMany2Bucket = PgxBulkSetSingleBuilder(tname)(p)
			b.Run("empty pair", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sb(context.Background(), nil)
						if nil != e {
							b.Errorf("Should be nop")
						}
					}
				})
			})

			b.Run("single pair", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					var i uint64 = 0
					buf8 := make([]byte, 8)
					pairs := []s2k.Pair{
						{
							Key: nil,
							Val: nil,
						},
					}
					for pb.Next() {
						binary.LittleEndian.PutUint64(buf8, i)
						pairs[0].Key = buf8
						e := sb(context.Background(), pairs)
						if nil != e {
							b.Errorf("Unable to add key/val: %v", e)
						}
						i += 1
						b.SetBytes(8)
					}
				})
			})
		})
	})

	b.Cleanup(func() {
		p.Close()
	})
}

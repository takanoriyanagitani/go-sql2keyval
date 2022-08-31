package pgx2kv

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

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

	var dt s2k.DelBucket = PgxDelBucketNew(p)
	var ab s2k.AddBucket = PgxAddBucketNew(p)
	var sm s2k.SetMany = PgxBulkSetNew(p)

	tname := "benchmark_setmany"

	e = dt(context.Background(), tname)
	if nil != e {
		b.Errorf("Unable to drop test table: %v", e)
	}

	e = ab(context.Background(), tname)
	if nil != e {
		b.Errorf("Unable to create test table: %v", e)
	}

	tnames := []string{
		"test_batch1",
		"test_batch2",
		"test_batch3",
		"test_dates",
		"test_devices",
		"test_devices_2022_08_31",
		"test_dates_cafef00ddeadbeafface864299792458",
		"test_data_2022_08_31_cafef00ddeadbeafface864299792458",
	}

	var integers s2k.Iter[int] = s2k.IterInts(0, 256)
	var pnames s2k.Iter[string] = s2k.IterMap(integers, func(i int) string {
		return fmt.Sprintf("c%03df00ddeadbeafface864299792458", i)
	})

	var randVal []byte = make([]byte, 16384)
	_, e = rand.Read(randVal)
	if nil != e {
		b.Errorf("Unable to initialize random val: %v", e)
	}

	var ibmany s2k.Iter[s2k.Batch] = s2k.IterMap(pnames, func(tname string) s2k.Batch {
		return s2k.BatchNew(tname, []byte("key"), randVal)
	})

	bmany := ibmany.ToArray()

	for _, tn := range tnames {
		e := ab(context.Background(), tn)
		if nil != e {
			b.Errorf("Unable to create table: %v", e)
		}
	}

	for _, ba := range bmany {
		e := ab(context.Background(), ba.Bucket())
		if nil != e {
			b.Errorf("Unable to create table: %v", e)
		}
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

		b.Run("single bucket, many iter key,value", func(b *testing.B) {
			var sb s2k.Pairs2Bucket = PgxPairs2BucketSingleBuilder(tname)(p)
			b.Run("empty pair", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sb(context.Background(), s2k.IterEmptyNew[s2k.Pair]())
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
					pair := s2k.Pair{
						Key: nil,
						Val: nil,
					}
					pi := 0
					pairs := func() s2k.Option[s2k.Pair] {
						if 0 == pi {
							pi += 1
							binary.LittleEndian.PutUint64(buf8, i)
							pair.Key = buf8
							return s2k.OptionNew(pair)
						}
						return s2k.OptionEmptyNew[s2k.Pair]()
					}
					for pb.Next() {
						e := sb(context.Background(), pairs)
						if nil != e {
							b.Errorf("Unable to add key/val: %v", e)
						}
						i += 1
						pi = 0
						b.SetBytes(8)
					}
				})
			})

			scale2bench := func(scale uint64) func(b *testing.B) {
				return func(b *testing.B) {
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						var i uint64 = 0
						buf8 := make([]byte, 8)
						pair := s2k.Pair{
							Key: nil,
							Val: nil,
						}
						var pi uint64 = 0
						pairs := func() s2k.Option[s2k.Pair] {
							if pi < scale {
								pi += 1
								binary.LittleEndian.PutUint64(buf8, uint64(time.Now().UnixNano()))
								pair.Key = buf8
								return s2k.OptionNew(pair)
							}
							return s2k.OptionEmptyNew[s2k.Pair]()
						}
						for pb.Next() {
							e := sb(context.Background(), pairs)
							if nil != e {
								b.Errorf("Unable to add key/val: %v", e)
							}
							i += 1
							pi = 0
							b.SetBytes(int64(8 * scale))
						}
					})
					b.ReportMetric(
						float64(b.N)*float64(scale),
						"inserts",
					)
				}
			}

			scales := []uint64{
				1,
				16,
				128,
				1024,
				16384,
			}

			for _, scale := range scales {
				b.Run(fmt.Sprintf("%v pairs", scale), scale2bench(scale))
			}
		})

		b.Run("many batch", func(b *testing.B) {
			var sb s2k.SetBatch = PgxBatchUpsertNew(p)

			b.Run("empty batch", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sb(context.Background(), s2k.IterEmptyNew[s2k.Batch]())
						if nil != e {
							b.Errorf("Unexpected error: %v", e)
						}
					}
				})
			})

			b.Run("single batch", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sb(context.Background(), s2k.IterFromArray([]s2k.Batch{
							s2k.BatchNew(tnames[0], []byte("k"), nil),
						}))
						if nil != e {
							b.Errorf("Unexpected error: %v", e)
						}
					}
				})
			})

			b.Run("multi batch", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						e := sb(context.Background(), s2k.IterFromArray([]s2k.Batch{
							s2k.BatchNew(tnames[0], []byte("k"), nil),
							s2k.BatchNew(tnames[1], []byte("k"), nil),
							s2k.BatchNew(tnames[2], []byte("k"), nil),
						}))
						if nil != e {
							b.Errorf("Unexpected error: %v", e)
						}
					}
				})
			})

			b.Run("many", func(b *testing.B) {
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						buf8 := make([]byte, 8)
						ib := s2k.IterFromArray(bmany)
						mapd := s2k.IterMap(ib, func(b s2k.Batch) s2k.Batch {
							i := uint64(time.Now().UnixNano())
							binary.LittleEndian.PutUint64(buf8, i)
							return b.WithKey(buf8)
						})
						e := sb(context.Background(), mapd)
						if nil != e {
							b.Errorf("Unexpected error: %v", e)
						}
					}
				})
				b.ReportMetric(
					float64(b.N)*float64(len(bmany)),
					"inserts",
				)
			})
		})
	})

	b.Cleanup(func() {
		p.Close()
	})
}

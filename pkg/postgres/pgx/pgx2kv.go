package pgx2kv

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"text/template"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func pgxSetTranBuilderNew(qgen QueryGenerator) func(t pgx.Tx) s2k.Set {
	return func(t pgx.Tx) s2k.Set {
		return func(ctx context.Context, bucket string, key, val []byte) error {
			q, e := qgen(bucket)
			if nil != e {
				return e
			}
			_, e = t.Exec(ctx, q, key, val)
			return e
		}
	}
}

type bufQueryGen func(bucket string) func(buf *strings.Builder) (query string, e error)

var queryStrPool = sync.Pool{
	New: func() any {
		return new(strings.Builder)
	},
}

func pgxBatchUpsertBuilderNew(qgen bufQueryGen) func(t pgx.Tx) s2k.SetBatch {
	return func(t pgx.Tx) s2k.SetBatch {
		return func(ctx context.Context, many s2k.Iter[s2k.Batch]) error {
			var pb pgx.Batch

			var buf *strings.Builder = queryStrPool.Get().(*strings.Builder)
			defer queryStrPool.Put(buf)

			for o := many(); o.HasValue(); o = many() {
				var b s2k.Batch = o.Value()
				buf.Reset()
				q, e := qgen(b.Bucket())(buf)
				if nil != e {
					return e
				}
				pb.Queue(q, b.Pair().Key, b.Pair().Val)
			}

			l := pb.Len()
			results := t.SendBatch(ctx, &pb)
			defer results.Close()

			for i := 0; i < l; i++ {
				_, e := results.Exec()
				if nil != e {
					return e
				}
			}

			return nil
		}
	}
}

type builtQuery struct {
	query string
	e     error
}

func pgxSetTranBuilderSingleNew(query builtQuery) func(t pgx.Tx) s2k.Set2Bucket {
	return func(t pgx.Tx) s2k.Set2Bucket {
		return func(ctx context.Context, key, val []byte) error {
			if nil != query.e {
				return query.e
			}
			_, e := t.Exec(ctx, query.query, key, val)
			return e
		}
	}
}

func pgxBucketAddNew(qgen QueryGenerator) func(p *pgxpool.Pool) s2k.AddBucket {
	return func(p *pgxpool.Pool) s2k.AddBucket {
		return func(ctx context.Context, bucket string) error {
			q, e := qgen(bucket)
			if nil != e {
				return e
			}
			_, e = p.Exec(ctx, q)
			return e
		}
	}
}

func pgxLogAddNew(qgen QueryGenerator) func(p *pgxpool.Pool) s2k.AddLog {
	return func(p *pgxpool.Pool) s2k.AddLog {
		return func(ctx context.Context, bucket string) error {
			q, e := qgen(bucket)
			if nil != e {
				return e
			}
			_, e = p.Exec(ctx, q)
			return e
		}
	}
}

func pgxLogInsertNew(query builtQuery) func(p *pgxpool.Pool) s2k.InsLog {
	return func(p *pgxpool.Pool) s2k.InsLog {
		return func(ctx context.Context, lg []byte) error {
			if nil != query.e {
				return query.e
			}
			_, e := p.Exec(ctx, query.query, lg)
			return e
		}
	}
}

func pgxBucketDelNew(qgen QueryGenerator) func(p *pgxpool.Pool) s2k.DelBucket {
	return func(p *pgxpool.Pool) s2k.DelBucket {
		return func(ctx context.Context, bucket string) error {
			q, e := qgen(bucket)
			if nil != e {
				return e
			}
			_, e = p.Exec(ctx, q)
			return e
		}
	}
}

func poolExec(ctx context.Context, p *pgxpool.Pool, f func(pgx.Tx) error) error {
	c, err := p.Acquire(ctx)
	if nil != err {
		return err
	}
	defer c.Release()

	return c.BeginFunc(ctx, f)
}

func pgxSingleBulkSetBuilder(tx2setter func(pgx.Tx) s2k.Set2Bucket) func(*pgxpool.Pool) s2k.SetMany2Bucket {
	return func(p *pgxpool.Pool) s2k.SetMany2Bucket {
		return func(ctx context.Context, pairs []s2k.Pair) error {
			return poolExec(ctx, p, func(tx pgx.Tx) error {
				setter := tx2setter(tx)
				sm := s2k.NonAtomicSetsSingleNew(setter)
				return sm(ctx, pairs)
			})
		}
	}
}

func pgxPairs2BucketSingleBuilder(tx2setter func(pgx.Tx) s2k.Set2Bucket) func(*pgxpool.Pool) s2k.Pairs2Bucket {
	return func(p *pgxpool.Pool) s2k.Pairs2Bucket {
		return func(ctx context.Context, pairs s2k.Iter[s2k.Pair]) error {
			return poolExec(ctx, p, func(tx pgx.Tx) error {
				setter := tx2setter(tx)
				sm := s2k.NonAtomicPairs2BucketNew(setter)
				return sm(ctx, pairs)
			})
		}
	}
}

func pgxBulkSetBuilder(tx2setter func(pgx.Tx) s2k.Set) func(*pgxpool.Pool) s2k.SetMany {
	return func(p *pgxpool.Pool) s2k.SetMany {
		return func(ctx context.Context, bucket string, pairs []s2k.Pair) error {
			return poolExec(ctx, p, func(tx pgx.Tx) error {
				setter := tx2setter(tx)
				sm := s2k.NonAtomicSetsNew(setter)
				return sm(ctx, bucket, pairs)
			})
		}
	}
}

func pgxBatchUpsertBuilder(tx2setter func(pgx.Tx) s2k.SetBatch) func(*pgxpool.Pool) s2k.SetBatch {
	return func(p *pgxpool.Pool) s2k.SetBatch {
		return func(ctx context.Context, many s2k.Iter[s2k.Batch]) error {
			return poolExec(ctx, p, func(tx pgx.Tx) error {
				setter := tx2setter(tx)
				return setter(ctx, many)
			})
		}
	}
}

var pgxBulkSetNew func(qgen QueryGenerator) func(*pgxpool.Pool) s2k.SetMany = s2k.Compose(pgxSetTranBuilderNew, pgxBulkSetBuilder)
var pgxBulkSetSingleNew func(query builtQuery) func(*pgxpool.Pool) s2k.SetMany2Bucket = s2k.Compose(pgxSetTranBuilderSingleNew, pgxSingleBulkSetBuilder)
var pgxPairs2BucketSingleNew func(query builtQuery) func(*pgxpool.Pool) s2k.Pairs2Bucket = s2k.Compose(pgxSetTranBuilderSingleNew, pgxPairs2BucketSingleBuilder)
var pgxBatchUpsertNew func(qgen bufQueryGen) func(*pgxpool.Pool) s2k.SetBatch = s2k.Compose(pgxBatchUpsertBuilderNew, pgxBatchUpsertBuilder)

type tableValidator func(tableName string) error
type QueryGenerator func(bucketName string) (query string, e error)

func bucket2queryNew(qgen QueryGenerator) func(bucketName string) builtQuery {
	return func(bucketName string) builtQuery {
		return qgen.build(bucketName)
	}
}

func (q QueryGenerator) build(bucketName string) (b builtQuery) {
	b.query, b.e = q(bucketName)
	return
}

func queryGeneratorNew(v tableValidator, g QueryGenerator) QueryGenerator {
	return func(bucketName string) (query string, e error) {
		e = v(bucketName)
		if nil != e {
			return "", e
		}
		return g(bucketName)
	}
}

func bufQueryGeneratorNew(v tableValidator, g bufQueryGen) bufQueryGen {
	return func(bucketName string) func(buf *strings.Builder) (query string, e error) {
		e := v(bucketName)
		if nil != e {
			return func(_ *strings.Builder) (string, error) {
				return "", e
			}
		}
		return g(bucketName)
	}
}

func regexTableValidatorNew(re *regexp.Regexp) tableValidator {
	return func(tableName string) error {
		found := re.MatchString(tableName)
		if found {
			return nil
		}
		return fmt.Errorf("Invalid table name: %s", tableName)
	}
}

var patTableValidatorNewMust func(pat string) tableValidator = s2k.Compose(regexp.MustCompile, regexTableValidatorNew)

func str2templateMust(name string) func(s string) *template.Template {
	return func(s string) *template.Template {
		return template.Must(template.New(name).Parse(s))
	}
}

func templateQueryGeneratorNew(name string) func(t *template.Template) QueryGenerator {
	return func(t *template.Template) QueryGenerator {
		return func(bucketName string) (query string, e error) {
			var buf strings.Builder
			e = t.ExecuteTemplate(&buf, "root", map[string]string{"tableName": bucketName})
			return buf.String(), e
		}
	}
}

func bufTemplateQueryGeneratorNew(name string) func(t *template.Template) bufQueryGen {
	return func(t *template.Template) bufQueryGen {
		return func(bucketName string) func(buf *strings.Builder) (query string, e error) {
			return func(buf *strings.Builder) (query string, e error) {
				e = t.ExecuteTemplate(buf, "root", map[string]string{"tableName": bucketName})
				return buf.String(), e
			}
		}
	}
}

var strQueryGeneratorNewMust func(s string) QueryGenerator = s2k.Compose(
	str2templateMust("root"),
	templateQueryGeneratorNew("root"),
)

var bufQueryGeneratorNewMust func(s string) bufQueryGen = s2k.Compose(
	str2templateMust("root"),
	bufTemplateQueryGeneratorNew("root"),
)

var pgTableValidator tableValidator = patTableValidatorNewMust(`^[a-z][a-z0-9_]{0,58}$`)

const upsertQuery = `
	INSERT INTO {{.tableName}} AS alias_t
	VALUES($1, $2)
	ON CONFLICT ON CONSTRAINT {{.tableName}}_pkc
	DO UPDATE SET val=EXCLUDED.val
	WHERE alias_t.val <> EXCLUDED.val
`

var pgUpsertGenerator QueryGenerator = strQueryGeneratorNewMust(upsertQuery)
var pgBufUpsertGenerator bufQueryGen = bufQueryGeneratorNewMust(upsertQuery)

var pgSetQueryGenerator QueryGenerator = queryGeneratorNew(
	pgTableValidator,
	pgUpsertGenerator,
)

var pgBufSetQueryGenerator bufQueryGen = bufQueryGeneratorNew(
	pgTableValidator,
	pgBufUpsertGenerator,
)

var pgBulkAddQueryGenerator QueryGenerator = queryGeneratorNew(
	pgTableValidator,
	strQueryGeneratorNewMust(`
		CREATE TABLE IF NOT EXISTS {{.tableName}} (
		  key BYTEA,
		  val BYTEA,
		  CONSTRAINT {{.tableName}}_pkc PRIMARY KEY(key)
		)
	`),
)

var pgAddLogQueryGenerator QueryGenerator = queryGeneratorNew(
	pgTableValidator,
	strQueryGeneratorNewMust(`
		CREATE TABLE IF NOT EXISTS {{.tableName}} (
			id BIGSERIAL,
			lg BYTEA,
			CONSTRAINT {{.tableName}}_pkc PRIMARY KEY(id)
		)
	`),
)

var pgLogInsertQueryGenerator QueryGenerator = queryGeneratorNew(
	pgTableValidator,
	strQueryGeneratorNewMust(`
		INSERT INTO {{.tableName}} (lg)
		VALUES($1)
	`),
)

var pgBulkDelQueryGenerator QueryGenerator = queryGeneratorNew(
	pgTableValidator,
	strQueryGeneratorNewMust(`
		DROP TABLE IF EXISTS {{.tableName}}
	`),
)

var PgxBulkSetNew func(p *pgxpool.Pool) s2k.SetMany = pgxBulkSetNew(pgSetQueryGenerator)
var PgxAddBucketNew func(p *pgxpool.Pool) s2k.AddBucket = pgxBucketAddNew(pgBulkAddQueryGenerator)
var PgxDelBucketNew func(p *pgxpool.Pool) s2k.DelBucket = pgxBucketDelNew(pgBulkDelQueryGenerator)
var PgxAddLogNew func(p *pgxpool.Pool) s2k.AddLog = pgxLogAddNew(pgAddLogQueryGenerator)

var PgxBatchUpsertNew func(p *pgxpool.Pool) s2k.SetBatch = pgxBatchUpsertNew(pgBufSetQueryGenerator)

var PgxLogInsBuilder func(bucketName string) func(p *pgxpool.Pool) s2k.InsLog = s2k.Compose(
	bucket2queryNew(pgLogInsertQueryGenerator),
	pgxLogInsertNew,
)

var PgxBulkSetSingleBuilder func(bucketName string) func(p *pgxpool.Pool) s2k.SetMany2Bucket = s2k.Compose(
	bucket2queryNew(pgSetQueryGenerator),
	pgxBulkSetSingleNew,
)

var PgxPairs2BucketSingleBuilder func(bucketName string) func(p *pgxpool.Pool) s2k.Pairs2Bucket = s2k.Compose(
	bucket2queryNew(pgSetQueryGenerator),
	pgxPairs2BucketSingleNew,
)

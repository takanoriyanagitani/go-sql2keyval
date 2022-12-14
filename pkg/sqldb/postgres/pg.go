package pg

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func init() {
	qgen := newQueryGeneratorMust()
	s2k.RegisterQueryGenerator("postgres", &qgen)
}

type validator func(bucketName string) error

type queryGenerator struct {
	tableChecker validator
	tmpl         *template.Template
}

func newValidatorRegexpMust(pattern string) validator {
	re := regexp.MustCompile(pattern)
	return func(bucketName string) error {
		return s2k.Bool2error(
			re.MatchString(bucketName),
			func() error {
				return fmt.Errorf("Invalid bucket name: %s", bucketName)
			},
		)
	}
}

func newQueryGeneratorMust() queryGenerator {
	// 0:     first char: [a-z]
	// 1-58:  identifier
	// 59-62: _pkc(reserved)
	// 63:    null char(reserved)
	tableChecker := newValidatorRegexpMust(`^[a-z][0-9a-z_]{0,58}$`)
	tmpl := template.Must(template.New("root").Parse(`
	  {{define "Get"}}
		SELECT val FROM {{.tableName}}
		WHERE key=$1
		LIMIT 1
	  {{end}}

	  {{define "Lst"}}
		SELECT key FROM {{.tableName}}
		ORDER BY key
	  {{end}}

	  {{define "Del"}}
		DELETE FROM {{.tableName}}
		WHERE key=$1
	  {{end}}

	  {{define "Add"}}
		INSERT INTO {{.tableName}}(key, val)
		VALUES ($1, $2)
	  {{end}}

	  {{define "Set"}}
		INSERT INTO {{.tableName}} AS alias_insert (key, val)
		VALUES ($1, $2)
		ON CONFLICT ON CONSTRAINT {{.tableName}}_pkc
		DO UPDATE SET val=EXCLUDED.val
		WHERE alias_insert.val != EXCLUDED.val
	  {{end}}

	  {{define "BDel"}}
		DROP TABLE IF EXISTS {{.tableName}}
	  {{end}}

	  {{define "BAdd"}}
		CREATE TABLE IF NOT EXISTS {{.tableName}}(
		  key BYTEA,
		  val BYTEA NOT NULL,
		  CONSTRAINT {{.tableName}}_pkc PRIMARY KEY(key)
		)
	  {{end}}
	`))
	return queryGenerator{
		tableChecker,
		tmpl,
	}
}

func (q *queryGenerator) generate(bucket string, name string) (query string, e error) {
	e = q.tableChecker(bucket)
	if nil != e {
		return "", e
	}

	var buf strings.Builder
	e = q.tmpl.ExecuteTemplate(&buf, name, map[string]string{"tableName": bucket})
	query = buf.String()
	return
}

func (q *queryGenerator) Get(bucket string) (query string, e error)  { return q.generate(bucket, "Get") }
func (q *queryGenerator) Del(bucket string) (query string, e error)  { return q.generate(bucket, "Del") }
func (q *queryGenerator) Add(bucket string) (query string, e error)  { return q.generate(bucket, "Add") }
func (q *queryGenerator) Set(bucket string) (query string, e error)  { return q.generate(bucket, "Set") }
func (q *queryGenerator) Lst(bucket string) (query string, e error)  { return q.generate(bucket, "Lst") }
func (q *queryGenerator) DelBucket(b string) (query string, e error) { return q.generate(b, "BDel") }
func (q *queryGenerator) AddBucket(b string) (query string, e error) { return q.generate(b, "BAdd") }

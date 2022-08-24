package stdsql

import (
	"context"
	"database/sql"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func dbOpenNew(driverName string) func(conn string) (*sql.DB, error) {
	return func(conn string) (*sql.DB, error) {
		return sql.Open(driverName, conn)
	}
}

func QueryNew(d *sql.DB) s2k.Query {
	return func(ctx context.Context, query string, args ...any) s2k.Record {
		return d.QueryRowContext(ctx, query, args)
	}
}

func ExecNew(d *sql.DB) s2k.Exec {
	return func(ctx context.Context, query string, args ...any) error {
		_, e := d.ExecContext(ctx, query, args)
		return e
	}
}

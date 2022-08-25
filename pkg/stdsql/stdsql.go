package stdsql

import (
	"context"
	"database/sql"
	"fmt"

	s2k "github.com/takanoriyanagitani/go-sql2keyval"
)

func DbOpenNew(driverName string) func(conn string) (*sql.DB, error) {
	return func(conn string) (*sql.DB, error) {
		return sql.Open(driverName, conn)
	}
}

func QueryNew(d *sql.DB) s2k.Query {
	return func(ctx context.Context, query string, args ...any) s2k.Record {
		return d.QueryRowContext(ctx, query, args...)
	}
}

func ExecNew(d *sql.DB) s2k.Exec {
	return func(ctx context.Context, query string, args ...any) error {
		_, e := d.ExecContext(ctx, query, args...)
		if nil == e {
			return nil
		}
		return fmt.Errorf("Unable to execute query(arg len: %v, q: %s): %v", len(args), query, e)
	}
}

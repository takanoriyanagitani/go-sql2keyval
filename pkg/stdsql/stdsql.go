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

func QueryCbNew(d *sql.DB) s2k.QueryCb {
	return func(ctx context.Context, cb s2k.RecordConsumer, query string, args ...any) error {
		rows, e := d.QueryContext(ctx, query, args...)
		if nil != e {
			return fmt.Errorf("Unable to get rows: %v", e)
		}
		defer rows.Close()

		for rows.Next() {
			e = cb(rows)
			if nil != e {
				return fmt.Errorf("Unable to process row: %v", e)
			}
		}
		return rows.Err()
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

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RowsCompat implements Rows to maintain the compatibility with sql.Rows.
type RowsCompat struct {
	rows    *sql.Rows
	pgxRows pgx.Rows
}

func (r *RowsCompat) Close() error {
	if r.pgxRows != nil {
		r.pgxRows.Close()
		return nil
	}
	return r.rows.Close()
}

func (r *RowsCompat) Err() error {
	if r.pgxRows != nil {
		return r.pgxRows.Err()
	}
	return r.rows.Close()
}

func (r *RowsCompat) Next() bool {
	if r.pgxRows != nil {
		return r.pgxRows.Next()
	}
	return r.rows.Next()
}

func (r *RowsCompat) Scan(dest ...any) error {
	if r.pgxRows != nil {
		err := r.pgxRows.Scan(dest...)
		if err != nil {
			err = fmt.Errorf("pgx_rowscompat: %w", err)
		}
		return err
	}
	return r.rows.Scan(dest...)
}

type RowCompat struct {
	row    *sql.Row
	pgxRow pgx.Row
}

func (r *RowCompat) Scan(dest ...any) error {
	if r.pgxRow != nil {
		return r.pgxRow.Scan(dest...)
	}
	return r.row.Scan(dest...)
}

func sqlIsoLevelToPgxIsoLevel(iso sql.IsolationLevel) pgx.TxIsoLevel {
	switch iso {
	case sql.LevelReadCommitted:
		return pgx.ReadCommitted
	case sql.LevelReadUncommitted:
		return pgx.ReadUncommitted
	case sql.LevelRepeatableRead:
		return pgx.RepeatableRead
	case sql.LevelSerializable:
		return pgx.Serializable
	case sql.LevelLinearizable:
	}
	// The default transaction isolation mode is ReadCommitted, you can read more at this page:
	// https://www.postgresql.org/docs/current/transaction-iso.html#:~:text=Read%20Committed%20is%20the%20default%20isolation%20level%20in%20PostgreSQL.
	return pgx.ReadCommitted
}

type ExecResultCompat struct {
	result    sql.Result
	pgxResult pgconn.CommandTag
}

// LastInsertId returns the latest id from inserts. The PostgreSQL protocol does not really support this, so this function
// will always return (0,nil).
//
// User can always use 'RETURNING' to get the latest insert id.
func (e *ExecResultCompat) LastInsertId() (int64, error) {
	return 0, nil
}

func (e *ExecResultCompat) RowsAffected() (int64, error) {
	if e.result != nil {
		return e.result.RowsAffected()
	}
	rowsAffected := e.pgxResult.RowsAffected()
	return rowsAffected, nil
}

type StmtCompat struct {
	sql   string
	pgxdb *pgxpool.Pool
	stmt  *sql.Stmt
}

func (s *StmtCompat) QueryContext(ctx context.Context, args ...any) (*RowsCompat, error) {
	if s.pgxdb != nil {
		rows, err := s.pgxdb.Query(ctx, s.sql, args...)
		if err != nil {
			return nil, err
		}
		return &RowsCompat{pgxRows: rows}, nil
	}
	rows, err := s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &RowsCompat{rows: rows}, nil
}

func (s *StmtCompat) ExecContext(ctx context.Context, args ...any) (*ExecResultCompat, error) {
	if s.pgxdb != nil {
		result, err := s.pgxdb.Exec(ctx, s.sql, args...)
		if err != nil {
			return nil, err
		}
		return &ExecResultCompat{pgxResult: result}, nil
	}
	result, err := s.stmt.ExecContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &ExecResultCompat{result: result}, nil
}

func (s *StmtCompat) Close() error {
	if s.pgxdb != nil {
		return nil
	}
	return s.stmt.Close()
}

// TransactCompat handle backwards compatibility guarantee of different postgreSQL libraries. The object doesn't really replicate the tx object
// for both pgx and database/sql because we will encapsulate the tx with another Postgres type.
type TransactCompat struct {
	tx    *sql.Tx
	pgxTx pgx.Tx
}

func (t *TransactCompat) Rollback() error {
	if t.pgxTx != nil {
		return t.pgxTx.Rollback(context.Background())
	}
	return t.tx.Rollback()
}

func (t *TransactCompat) Commit() error {
	if t.pgxTx != nil {
		return t.pgxTx.Commit(context.Background())
	}
	return t.tx.Commit()
}

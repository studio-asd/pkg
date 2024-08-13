package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

func (r *RowsCompat) Err() (err error) {
	defer func() {
		if err != nil {
			err = ErrToPostgresError(err)
		}
	}()
	if r.pgxRows != nil {
		err = r.pgxRows.Err()
		return
	}
	err = r.rows.Err()
	return
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
		// Append the error with sql.ErrNoRows so we can keep using errors.Is(error, sql.ErrNoRows).
		if err != nil {
			err = ErrToPostgresError(err)
			if errors.Is(err, pgx.ErrNoRows) {
				err = errors.Join(err, sql.ErrNoRows)
			}
			err = fmt.Errorf("pgx_rowscompat: %w", err)
		}
		return err
	}
	return ErrToPostgresError(r.rows.Scan(dest...))
}

type RowCompat struct {
	row    *sql.Row
	pgxRow pgx.Row
}

func (r *RowCompat) Scan(dest ...any) error {
	if r.pgxRow != nil {
		err := r.pgxRow.Scan(dest...)
		// Append the error with sql.ErrNoRows so we can keep using errors.Is(error, sql.ErrNoRows).
		if err != nil {
			err = ErrToPostgresError(err)
			if errors.Is(err, pgx.ErrNoRows) {
				err = errors.Join(err, sql.ErrNoRows)
			}
		}
		return err
	}
	return ErrToPostgresError(r.row.Scan(dest...))
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
	sql         string
	pgxdb       *pgxpool.Pool
	pgxTx       pgx.Tx
	pgxStmtDesc *pgconn.StatementDescription

	stmt *sql.Stmt

	ctx    context.Context
	tracer *TracerConfig
}

func (s *StmtCompat) Query(ctx context.Context, args ...any) (rc *RowsCompat, err error) {
	spanCtx, span := s.tracer.Tracer.Start(
		ctx,
		"postgres.query",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	span.SetAttributes(s.tracer.traceAttributesFromContext(ctx, "", args...)...)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			err = ErrToPostgresError(err)
		}
		span.End()
	}()

	if s.pgxTx != nil {
		rows, queryErr := s.pgxTx.Query(spanCtx, s.pgxStmtDesc.SQL, args...)
		if queryErr != nil {
			err = queryErr
			return
		}
		rc = &RowsCompat{pgxRows: rows}
		return
	}
	if s.pgxdb != nil {
		rows, queryErr := s.pgxdb.Query(spanCtx, s.sql, args...)
		if queryErr != nil {
			err = queryErr
			return
		}
		rc = &RowsCompat{pgxRows: rows}
		return
	}
	rows, queryErr := s.stmt.QueryContext(spanCtx, args...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rc = &RowsCompat{rows: rows}
	return
}

func (s *StmtCompat) Exec(ctx context.Context, args ...any) (er *ExecResultCompat, err error) {
	spanCtx, span := s.tracer.Tracer.Start(
		ctx,
		"postgres.exec",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			err = ErrToPostgresError(err)
		}
		span.End()
	}()

	if s.pgxTx != nil {
		result, execErr := s.pgxTx.Exec(spanCtx, s.pgxStmtDesc.SQL, args...)
		if execErr != nil {
			err = execErr
			return
		}
		er = &ExecResultCompat{pgxResult: result}
		return
	}
	if s.pgxdb != nil {
		result, execErr := s.pgxdb.Exec(spanCtx, s.sql, args...)
		if execErr != nil {
			err = execErr
			return
		}
		er = &ExecResultCompat{pgxResult: result}
		return
	}
	result, execErr := s.stmt.ExecContext(ctx, args...)
	if execErr != nil {
		err = execErr
		return
	}
	er = &ExecResultCompat{result: result}
	return
}

func (s *StmtCompat) Close() (err error) {
	_, span := s.tracer.Tracer.Start(
		s.ctx,
		"postgres.query",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if s.pgxdb != nil || s.pgxTx != nil {
		return nil
	}
	err = s.stmt.Close()
	return
}

// TransactCompat handle backwards compatibility guarantee of different postgreSQL libraries. The object doesn't really replicate the tx object
// for both pgx and database/sql because we will encapsulate the tx with another Postgres type.
type TransactCompat struct {
	tx    *sql.Tx
	pgxTx pgx.Tx

	ctx       context.Context
	tracer    *TracerConfig
	spanAttrs []attribute.KeyValue
}

func (t *TransactCompat) Rollback() (err error) {
	spanCtx, span := t.tracer.Tracer.Start(
		t.ctx,
		"postgres.rollback",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(t.spanAttrs...),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			err = ErrToPostgresError(err)
		}
		span.End()
	}()

	if t.pgxTx != nil {
		err = t.pgxTx.Rollback(spanCtx)
		return
	}
	err = t.tx.Rollback()
	return
}

func (t *TransactCompat) Commit() (err error) {
	spanCtx, span := t.tracer.Tracer.Start(
		t.ctx,
		"postgres.commit",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(t.spanAttrs...),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			err = ErrToPostgresError(err)
		}
		span.End()
	}()

	if t.pgxTx != nil {
		err = t.pgxTx.Commit(spanCtx)
		return
	}
	err = t.tx.Commit()
	return
}

func (t *TransactCompat) Exec(ctx context.Context, query string, args ...any) (*ExecResultCompat, error) {
	if t.pgxTx != nil {
		ct, err := t.pgxTx.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &ExecResultCompat{pgxResult: ct}, nil
	}
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &ExecResultCompat{result: result}, nil
}

func (t *TransactCompat) Query(ctx context.Context, query string, args ...any) (*RowsCompat, error) {
	if t.pgxTx != nil {
		rows, err := t.pgxTx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &RowsCompat{pgxRows: rows}, nil
	}
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &RowsCompat{rows: rows}, nil
}

func (t *TransactCompat) QueryRow(ctx context.Context, query string, args ...any) *RowCompat {
	if t.pgxTx != nil {
		row := t.pgxTx.QueryRow(ctx, query, args...)
		return &RowCompat{pgxRow: row}
	}
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &RowCompat{row: row}
}

func (t *TransactCompat) Prepare(ctx context.Context, query string) (*StmtCompat, error) {
	if t.pgxTx != nil {
		// We currently put the cached query name the same as the query. This will definitely make the cache-key(map) to be
		// very big and inefficient.
		//
		// Probably we should just expose name in the future, so the user can define it. But the stdlib doesn't actually support
		// this, but maybe that's okay.
		stmtDesc, err := t.pgxTx.Prepare(ctx, query, query)
		if err != nil {
			return nil, err
		}
		return &StmtCompat{
			ctx:         t.ctx,
			pgxTx:       t.pgxTx,
			pgxStmtDesc: stmtDesc,
			tracer:      t.tracer,
		}, nil
	}
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &StmtCompat{
		ctx:    t.ctx,
		stmt:   stmt,
		tracer: t.tracer,
	}, nil
}

// SpanAttributes returns all attributes of the spans inside a transaction. The span is saved inside the transaction because
// a single session is used within a transaction, and it have the same attributes across the session.
func (t *TransactCompat) SpanAttributes() []attribute.KeyValue {
	return t.spanAttrs
}

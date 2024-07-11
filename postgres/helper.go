// Helper contains modified codes from Go's pkgsite.
//
// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const OnConflictDoNothing = "ON CONFLICT DO NOTHING"

// BulkInsert constructs and executes a multi-value insert statement. The
// query is constructed using the format:
//
//	INSERT INTO <table> (<columns>) VALUES (<placeholders-for-each-item-in-values>)
//
// If conflictAction is not empty, it is appended to the statement.
//
// The query is executed using a PREPARE statement with the provided values.
func (p *Postgres) BulkInsert(ctx context.Context, table string, columns []string, values []interface{}, conflictAction string) (err error) {
	return p.bulkInsert(ctx, table, columns, nil, values, conflictAction, nil)
}

// BulkInsertReturning is like BulkInsert, but supports returning values from the INSERT statement.
// In addition to the arguments of BulkInsert, it takes a list of columns to return and a function
// to scan those columns. To get the returned values, provide a function that scans them as if
// they were the selected columns of a query. See TestBulkInsert for an example.
func (p *Postgres) BulkInsertReturning(ctx context.Context, table string, columns []string, values []interface{}, conflictAction string, returningColumns []string, scanFunc func(*RowsCompat) error) (err error) {
	if returningColumns == nil || scanFunc == nil {
		return errors.New("need returningColumns and scan function")
	}
	return p.bulkInsert(ctx, table, columns, returningColumns, values, conflictAction, scanFunc)
}

// BulkUpsert is like BulkInsert, but instead of a conflict action, a list of
// conflicting columns is provided. An "ON CONFLICT (conflict_columns) DO
// UPDATE" clause is added to the statement, with assignments "c=excluded.c" for
// every column c.
func (p *Postgres) BulkUpsert(ctx context.Context, table string, columns []string, values []interface{}, conflictColumns []string) error {
	conflictAction := buildUpsertConflictAction(columns, conflictColumns)
	return p.BulkInsert(ctx, table, columns, values, conflictAction)
}

func processRows(rows *RowsCompat, f func(*RowsCompat) error) error {
	defer rows.Close()
	for rows.Next() {
		if err := f(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (p *Postgres) bulkInsert(ctx context.Context, table string, columns, returningColumns []string, values []interface{}, conflictAction string, scanFunc func(*RowsCompat) error) (err error) {
	if remainder := len(values) % len(columns); remainder != 0 {
		return fmt.Errorf("modulus of len(values) and len(columns) must be 0: got %d", remainder)
	}

	// Postgres supports up to 65535 parameters, but stop well before that
	// so we don't construct humongous queries.
	const maxParameters = 1000
	stride := (maxParameters / len(columns)) * len(columns)
	if stride == 0 {
		// This is a pathological case (len(columns) > maxParameters), but we
		// handle it cautiously.
		return fmt.Errorf("too many columns to insert: %d", len(columns))
	}

	spanCtx, span := p.tracer.Start(
		ctx,
		"postgres.bulkInsert",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
	} else {
		span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, "")...)
	}

	prepare := func(n int) (*StmtCompat, error) {
		return p.Prepare(spanCtx, buildInsertQuery(table, columns, returningColumns, n, conflictAction))
	}

	var stmt *StmtCompat
	defer func() {
		var stmtErr error
		if stmt != nil {
			stmtErr = stmt.Close()
		}
		// Append the statment close error to the original error.
		if stmtErr != nil {
			err = errors.Join(err, stmtErr)
		}
	}()

	for leftBound := 0; leftBound < len(values); leftBound += stride {
		rightBound := leftBound + stride
		if rightBound <= len(values) && stmt == nil {
			stmt, err = prepare(stride)
			if err != nil {
				return err
			}
		} else if rightBound > len(values) {
			rightBound = len(values)
			stmt, err = prepare(rightBound - leftBound)
			if err != nil {
				return err
			}
		}
		valueSlice := values[leftBound:rightBound]
		var err error
		if returningColumns == nil {
			_, err = stmt.Exec(spanCtx, valueSlice...)
		} else {
			var rows *RowsCompat
			rows, err = stmt.Query(spanCtx, valueSlice...)
			if err != nil {
				return err
			}
			err = processRows(rows, scanFunc)
		}
		if err != nil {
			return fmt.Errorf("running bulk insert query, values[%d:%d]): %w", leftBound, rightBound, err)
		}
	}
	return nil
}

// buildInsertQuery builds an multi-value insert query, following the format:
// INSERT TO <table> (<columns>) VALUES (<placeholders-for-each-item-in-values>) <conflictAction>
// If returningColumns is not empty, it appends a RETURNING clause to the query.
//
// When calling buildInsertQuery, it must be true that nvalues % len(columns) == 0.
func buildInsertQuery(table string, columns, returningColumns []string, nvalues int, conflictAction string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s", table)
	fmt.Fprintf(&b, "(%s) VALUES", strings.Join(columns, ", "))

	var placeholders []string
	for i := 1; i <= nvalues; i++ {
		// Construct the full query by adding placeholders for each
		// set of values that we want to insert.
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		if i%len(columns) != 0 {
			continue
		}

		// When the end of a set is reached, write it to the query
		// builder and reset placeholders.
		fmt.Fprintf(&b, "(%s)", strings.Join(placeholders, ", "))
		placeholders = nil

		// Do not add a comma delimiter after the last set of values.
		if i == nvalues {
			break
		}
		b.WriteString(", ")
	}
	if conflictAction != "" {
		b.WriteString(" " + conflictAction)
	}
	if len(returningColumns) > 0 {
		fmt.Fprintf(&b, " RETURNING %s", strings.Join(returningColumns, ", "))
	}
	return b.String()
}

func buildUpsertConflictAction(columns, conflictColumns []string) string {
	var sets []string
	for _, c := range columns {
		sets = append(sets, fmt.Sprintf("%s=excluded.%[1]s", c))
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s",
		strings.Join(conflictColumns, ", "),
		strings.Join(sets, ", "))
}

// maxBulkUpdateArrayLen is the maximum size of an array that BulkUpdate will send to
// Postgres. (Postgres has no size limit on arrays, but we want to keep the statements
// to a reasonable size.)
// It is a variable for testing.
var maxBulkUpdateArrayLen = 10000

// BulkUpdate executes multiple UPDATE statements in a transaction.
//
// Columns must contain the names of some of table's columns. The first is treated
// as a key; that is, the values to update are matched with existing rows by comparing
// the values of the first column.
//
// Types holds the database type of each column. For example,
//
//	[]string{"INT", "TEXT"}
//
// Values contains one slice of values per column. (Note that this is unlike BulkInsert, which
// takes a single slice of interleaved values.)
func (p *Postgres) BulkUpdate(ctx context.Context, table string, columns, types []string, values [][]any) (err error) {
	if len(columns) < 2 {
		return errors.New("need at least two columns")
	}
	if len(columns) != len(values) {
		return errors.New("len(values) != len(columns)")
	}
	nRows := len(values[0])
	for _, v := range values[1:] {
		if len(v) != nRows {
			return errors.New("all values slices must be the same length")
		}
	}

	query := buildBulkUpdateQuery(table, columns, types)

	spanCtx, span := p.tracer.Start(
		ctx,
		"postgres.bulkUpdate",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
	} else {
		span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, "-")...)
	}

	for left := 0; left < nRows; left += maxBulkUpdateArrayLen {
		right := left + maxBulkUpdateArrayLen
		if right > nRows {
			right = nRows
		}
		var args []any
		for _, vs := range values {
			args = append(args, pq.Array(vs[left:right]))
		}
		if _, err := p.Exec(spanCtx, query, args...); err != nil {
			return fmt.Errorf("db.Exec(%q, values[%d:%d]): %w", query, left, right, err)
		}
	}
	return nil
}

func buildBulkUpdateQuery(table string, columns, types []string) string {
	var sets, unnests []string
	// Build "c = data.c" for each non-key column.
	for _, c := range columns[1:] {
		sets = append(sets, fmt.Sprintf("%s = data.%[1]s", c))
	}
	// Build "UNNEST($1::TYPE) AS c" for each column.
	// We need the type, or Postgres complains that UNNEST is not unique.
	for i, c := range columns {
		unnests = append(unnests, fmt.Sprintf("UNNEST($%d::%s[]) AS %s", i+1, types[i], c))
	}
	return fmt.Sprintf(`
		UPDATE %[1]s
		SET %[2]s
		FROM (SELECT %[3]s) AS data
		WHERE %[1]s.%[4]s = data.%[4]s`,
		table,                       // 1
		strings.Join(sets, ", "),    // 2
		strings.Join(unnests, ", "), // 3
		columns[0],                  // 4
	)
}

// emptyStringScanner wraps the functionality of sql.NullString to just write
// an empty string if the value is NULL.
type emptyStringScanner struct {
	ptr *string
}

func (e emptyStringScanner) Scan(value any) error {
	var ns sql.NullString
	if err := ns.Scan(value); err != nil {
		return err
	}
	*e.ptr = ns.String
	return nil
}

// NullIsEmpty returns a sql.Scanner that writes the empty string to s if the
// sql.Value is NULL.
func NullIsEmpty(s *string) sql.Scanner {
	return emptyStringScanner{s}
}

// CreateDatabase creates a database from the current connected user. To do this action, please ensure your user has the priviledge
// to create a new database.
func (p *Postgres) CreateDatabase(ctx context.Context, name string) error {
	query := fmt.Sprintf("CREATE DATABASE %s;", name)
	_, err := p.Exec(ctx, query)
	return err
}

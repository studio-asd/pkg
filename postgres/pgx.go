package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// CopyFromRows use PostgreSQL COPY protocol to inserts data into the database. Overall, COPY is faster than INSERT, and should
// be used if there are no rows conflict.
//
// Copy protocol documentation: https://www.postgresql.org/docs/current/sql-copy.html#:~:text=COPY%20moves%20data%20between%20PostgreSQL,results%20of%20a%20SELECT%20query.
func (p *Postgres) CopyFromRows(ctx context.Context, table string, columns []string, values [][]interface{}) (rnum int64, err error) {
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.pgx.copyFromRows",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if !p.IsPgx() {
		err = errors.New("copyFromRows can only be used when using pgx driver")
		return
	}
	// PGX cannot receive columns with uppercase, if there is a column named columnA and we try to query them with columnA then the
	// query will fail. In general PostgreSQL columns are case insensitive when we don't use double quote(") when doing query, for
	// example SELECT * from some_table where "columnA" = $1.
	//
	// To avoid this issue, then we should convert all the columns to lower case.
	for idx := range columns {
		columns[idx] = strings.ToLower(columns[idx])
	}

	rnum, err = p.pgx.CopyFrom(
		spanCtx,
		pgx.Identifier{table},
		columns,
		pgx.CopyFromRows(values),
	)
	return
}

// IsPgx returns true if pgx is being used.
func (p *Postgres) IsPgx() bool {
	return p.pgx != nil
}

// queryArgsToStringSlice returns args with type of any to a slice of string.
func queryArgsToStringSlice(args []any) []string {
	strAttributes := make([]string, len(args))
	for idx, arg := range args {
		switch v := arg.(type) {
		case string:
			strAttributes[idx] = v
		case *string:
			strAttributes[idx] = *v
		case int:
			strAttributes[idx] = strconv.Itoa(v)
		case int32:
			strAttributes[idx] = strconv.FormatInt(int64(v), 10)
		case int64:
			strAttributes[idx] = strconv.FormatInt(int64(v), 10)
		case float32:
			strAttributes[idx] = strconv.FormatFloat(float64(v), 'f', 0, 32)
		case float64:
			strAttributes[idx] = strconv.FormatFloat(float64(v), 'f', 0, 64)
		default:
			// Unfortunately, this part will allocate quite significantly as data.Args is []any, and each 'any'
			// might not be string. We cannot convert all of them to string via reflect either as there are too
			// many posibilities of how pgx construct their arguments. This means we can only do a best effrot
			// of doing %v to print the type verbosely.
			strAttributes[idx] = fmt.Sprintf("%v", v)
		}
	}
	return strAttributes
}

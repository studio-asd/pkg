package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/albertwidi/pkg/instrumentation"
)

var _ pgx.QueryTracer = (*PgxQueryTracer)(nil)

// CopyFromRows use PostgreSQL COPY protocol to inserts data into the database. Overall, COPY is faster than INSERT, and should
// be used if there are no rows conflict.
//
// Copy protocol documentation: https://www.postgresql.org/docs/current/sql-copy.html#:~:text=COPY%20moves%20data%20between%20PostgreSQL,results%20of%20a%20SELECT%20query.
func (p *Postgres) CopyFromRows(ctx context.Context, table string, columns []string, values [][]interface{}) (rnum int64, err error) {
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
		ctx,
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

// PgxQueryTracer is the default tracer for pgx. Internally it uses open-telemetry to trace all the queries.
type PgxQueryTracer struct {
	tracer trace.Tracer
	// excludeArgs will exlude the arguments from the tracing label. This means the arguments won't be visible
	// inside the tracing view UI.
	excludeArgs bool
	// dbInfo contains database connection information to give more context/attributes to the tracer. Even though
	// we have this information from pgx.Conn, the information is still in partial and we need to convert some of
	// the information to string. Instead of doing that, pass all the information here.
	dbInfo struct {
		user    string
		host    string
		port    string
		dbName  string
		sslMode string
	}
}

// TraceQueryStart will always be called at the start of query. The returned context from the function is meant to be propagated
// to TraceQueryEnd so we can continue the span by using the context.
func (t *PgxQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	// Retrieve the instrumentation baggage as we always set this to the context.
	bg := instrumentation.BaggageFromContext(ctx)
	attributes := []attribute.KeyValue{
		attribute.String("pg.user", t.dbInfo.user),
		attribute.String("pg.host", t.dbInfo.host),
		attribute.String("pg.port", t.dbInfo.port),
		attribute.String("pg.db_name", t.dbInfo.dbName),
		attribute.String("pg.sslmode", t.dbInfo.sslMode),
		attribute.String("pg.query", data.SQL),
		attribute.String("request.id", bg.RequestID),
		attribute.String("api.name", bg.APIName),
		attribute.String("api.owner", bg.APIOwner),
	}
	// If the arguments is available and not excluded, then we will add them into the trace.
	if data.Args != nil && !t.excludeArgs {
		attributes = append(attributes, attribute.StringSlice("pg.query.args", queryArgsToStringSlice(data.Args)))
	}

	// We will not end the trace because the query have not yet started. We won't get the trace duration data if
	// we end the trace here. Instead propagate the context and start a new span from context inside TraceQueryEnd
	// function.
	ctx, _ = t.tracer.Start(
		ctx,
		"pgx-otel",
		trace.WithAttributes(attributes...),
	)
	return ctx
}

func (t *PgxQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	attributes := []attribute.KeyValue{
		attribute.String("pg.command", data.CommandTag.String()),
	}
	if data.Err != nil {
		attributes = append(attributes, attribute.String("error", data.Err.Error()))
		// Add more information if we found the error is actually a PgError.
		var pgErr *pgconn.PgError
		if errors.As(data.Err, &pgErr) {
			attributes = append(
				attributes,
				attribute.String("pg_err.code", pgErr.Code),
			)
		}
	} else {
		attributes = append(attributes, attribute.Int64("pg.query.rows_affected", data.CommandTag.RowsAffected()))
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attributes...)
	span.End()
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
			strAttributes[idx] = strconv.FormatFloat(float64(v), 'f', 0, 32)
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

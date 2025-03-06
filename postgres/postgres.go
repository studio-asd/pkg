// Postgres is a compatibility layer between pgx and database/sql.
// This package also provide a pure pgx object if pgx is used.
//
// The library used pgxpool by default because pgxconn is not concurrently safe
// to be used by default. Pgxpool is a client-side connection pool implementation
// and not to be confused with something like PgBouncer. It is safe to use PgBouncer
// on top of pgxpool as PgBouncer v1.21 already supports prepared statements inside
// the transaction mode.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Postgres struct {
	config *ConnectConfig
	// tracer stores the pointer to the tracer configuration as we pass the tracer configuration everywhere.
	// We don't use pointer of the TracerConfig inside the ConnectConfig because the configuration can be copied
	// outside of the postgres package.
	//
	// As this configuration will be passed to transactions, etc. Please be awware to not change the values as
	// it will introduce a race.
	tracer *TracerConfig
	meter  *MeterConfig

	db    *sql.DB
	pgx   *pgxpool.Pool
	tx    Transaction
	txIso sql.IsolationLevel

	// cancelMonitorFn is a context cancel func to cancel/shutdown the monitoring gororutine.
	cancelMonitorFn context.CancelFunc
	// searchPathMu protects set of the searchpath.
	searchPathMu sync.RWMutex
	searchPath   string // default schema separated by comma.
	// closeMu protects closing postgres connection concurrently.
	closeMu     sync.Mutex
	closed      bool
	metricsName string
}

// InTransaction returns whether the postgres object is currently in transaction or not. The information need to be
// exposed via a function because we don't want to expose the 'tx' object.
func (p *Postgres) InTransaction() (ok bool, iso sql.IsolationLevel) {
	ok = p.tx != nil
	if ok {
		iso = p.txIso
	}
	return
}

// Config returns the copy of connection configuration.
func (p *Postgres) Config() ConnectConfig {
	return *p.config
}

// NewConfigFromDSN creates a new connect configuration from postgresql data source name.
func NewConfigFromDSN(dsn string) (ConnectConfig, error) {
	pgDSN, err := ParseDSN(dsn)
	if err != nil {
		return ConnectConfig{}, err
	}
	config := ConnectConfig{
		Driver:   "pgx",
		DBName:   pgDSN.DatabaseName,
		Username: pgDSN.Username,
		Password: pgDSN.Password,
		Host:     pgDSN.Host,
		Port:     pgDSN.Port,
		SSLMode:  pgDSN.SSLMode,
	}
	return config, config.validate()
}

// Connect returns connected Postgres object.
func Connect(ctx context.Context, config ConnectConfig) (*Postgres, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	var (
		db    *sql.DB
		pgxdb *pgxpool.Pool
		err   error
	)

	url, _, err := config.DSN()
	if err != nil {
		return nil, err
	}

	switch config.Driver {
	case "postgres":
		db, err = sql.Open(config.Driver, url)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(config.MaxOpenConns)
		db.SetConnMaxIdleTime(config.ConnMaxIdletime)
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	case "pgx":
		poolConfig, err := pgxpool.ParseConfig(url)
		if err != nil {
			return nil, err
		}
		poolConfig.MaxConns = int32(config.MaxOpenConns)
		poolConfig.MaxConnIdleTime = config.ConnMaxIdletime
		poolConfig.MaxConnLifetime = config.ConnMaxLifetime
		pgxdb, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			return nil, err
		}
	}

	monitorCtx, cancel := context.WithCancel(context.Background())
	p := &Postgres{
		// We copy the configuration because we are copying the Postgres object in every transactions and with metrics calls. And a direct
		// copy of the configuration struct will use quite a bit of memory.
		config:          &config,
		tracer:          &config.TracerConfig,
		meter:           &config.MeterConfig,
		db:              db,
		pgx:             pgxdb,
		searchPath:      config.SearchPath,
		cancelMonitorFn: cancel,
	}
	// Start the monitoring goroutine if monitor configuration is on, we want to monitor the number of connections
	// and the general stats of the postgres object.
	if config.MeterConfig.MonitorStats {
		go monitorPostgresStats(monitorCtx, p)
	}
	return p, nil
}

func (p *Postgres) Query(ctx context.Context, query string, params ...any) (*RowsCompat, error) {
	return p.query(ctx, query, params...)
}

func (p *Postgres) query(ctx context.Context, query string, params ...any) (rc *RowsCompat, err error) {
	mt := time.Now()
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.query",
		trace.WithSpanKind(trace.SpanKindInternal),
	)

	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres.func", "query"),
		}
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				newAttrs = append(newAttrs, attribute.String("postgres.err_code", code))
			}
		}

		if errRecord := p.recordMetrics(ctx, mt, newAttrs); errRecord != nil {
			err = errors.Join(err, errRecord)
		}
		span.SetAttributes(newAttrs...)
		span.End()
	}()

	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
		rc, err = p.tx.Query(spanCtx, query, params...)
		return
	}

	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, query, params...)...)
	if p.pgx != nil {
		rows, queryErr := p.pgx.Query(spanCtx, query, params...)
		if queryErr != nil {
			err = queryErr
			return
		}
		rc = &RowsCompat{pgxRows: rows}
		return
	}
	rows, queryErr := p.db.QueryContext(ctx, query, params...)
	if queryErr != nil {
		err = queryErr
		return
	}
	rc = &RowsCompat{rows: rows}
	return
}

func (p *Postgres) RunQuery(ctx context.Context, query string, f func(*RowsCompat) error, params ...any) (err error) {
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.runQuery",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				span.SetAttributes(
					attribute.String("pg.errCode", code),
				)
			}
		}
		// We don't record any  metrics here beucase we invoke p.query which will the record the metrics.
		span.End()
	}()

	var rows *RowsCompat
	rows, err = p.query(spanCtx, query, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if fErr := f(rows); fErr != nil {
			err = fErr
			return
		}
	}
	return
}

// QueryRow retrieve at most one row for a single query.
func (p *Postgres) QueryRow(ctx context.Context, query string, params ...any) *RowCompat {
	mt := time.Now()
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.queryRow",
		trace.WithSpanKind(trace.SpanKindInternal),
	)

	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres.func", "queryRow"),
		}
		// We need to silent the error here because the function signature don't return any error. We can return error if the implementation
		// of pgx and database/sql is identical. Unfortunately pgx doesn't allowed us to return any error before scanning.
		_ = p.recordMetrics(ctx, mt, newAttrs)
		span.SetAttributes(newAttrs...)
		span.End()
	}()

	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
		return p.tx.QueryRow(spanCtx, query, params...)
	}

	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, query, params...)...)
	// The pgx row is different from the database/sql implementation which allows the user to check whether there is
	// an error without scanning the row.
	if p.pgx != nil {
		row := p.pgx.QueryRow(spanCtx, query, params...)
		return &RowCompat{pgxRow: row}
	}
	row := p.db.QueryRowContext(spanCtx, query, params...)
	return &RowCompat{row: row}
}

func (p *Postgres) Exec(ctx context.Context, query string, params ...any) (ec *ExecResultCompat, err error) {
	mt := time.Now()

	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.exec",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres_func", "exec"),
		}
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				newAttrs = append(newAttrs, attribute.String("postgres.err_code", code))
			}
		}
		if errRecord := p.recordMetrics(ctx, mt, newAttrs); errRecord != nil {
			err = errors.Join(err, errRecord)
		}
		span.SetAttributes(newAttrs...)
		span.End()
	}()

	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
		return p.tx.Exec(spanCtx, query, params...)
	}

	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, query, params...)...)
	if p.pgx != nil {
		tag, execErr := p.pgx.Exec(spanCtx, query, params...)
		if execErr != nil {
			err = execErr
			return
		}
		ec = &ExecResultCompat{pgxResult: tag}
		return
	}
	result, execErr := p.db.ExecContext(spanCtx, query, params...)
	if execErr != nil {
		err = execErr
		return
	}
	ec = &ExecResultCompat{result: result}
	return
}

// Transaction interface ensure the pgx and sql/db tx object is compatible so we can use them both inside
// the Postgres object.
type Transaction interface {
	Rollback() error
	Commit() error
	Exec(ctx context.Context, query string, params ...any) (*ExecResultCompat, error)
	QueryRow(ctx context.Context, query string, params ...any) *RowCompat
	Query(ctx context.Context, query string, params ...any) (*RowsCompat, error)
	SpanAttributes() []attribute.KeyValue
	Prepare(ctx context.Context, query string) (*StmtCompat, error)
}

func (p *Postgres) Transact(ctx context.Context, iso sql.IsolationLevel, txFunc func(context.Context, *Postgres) error) error {
	err := p.transact(ctx, iso, txFunc)
	return err
}

func (p *Postgres) beginTx(ctx context.Context, iso sql.IsolationLevel) (tx *TransactCompat, err error) {
	spanAttrs := p.config.TracerConfig.traceAttributesFromContext(ctx, "")
	spanAttrs = append(
		spanAttrs,
		attribute.String("postgres.tx_iso_level", iso.String()),
		attribute.Bool("postgres.in_transaction", true),
	)

	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.beginTx",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(spanAttrs...),
	)
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if p.pgx != nil {
		var pgxTx pgx.Tx
		pgxTx, beginErr := p.pgx.BeginTx(spanCtx, pgx.TxOptions{IsoLevel: sqlIsoLevelToPgxIsoLevel(iso)})
		if beginErr != nil {
			err = beginErr
			return
		}
		tx = &TransactCompat{
			pgxTx:  pgxTx,
			ctx:    spanCtx,
			tracer: p.tracer,
			// Load span attributes once, without the query name and also the arguments. So we don't have to load
			// all the attributes again in each operation.
			spanAttrs: spanAttrs,
		}
		return
	}
	stdlibTx, beginErr := p.db.BeginTx(spanCtx, &sql.TxOptions{Isolation: iso})
	if beginErr != nil {
		err = beginErr
		return
	}
	tx = &TransactCompat{
		tx:     stdlibTx,
		ctx:    spanCtx,
		tracer: p.tracer,
		// Load span attributes once, without the query name and also the arguments. So we don't have to load
		// all the attributes again in each operation.
		spanAttrs: spanAttrs,
	}
	return
}

func (p *Postgres) transact(ctx context.Context, iso sql.IsolationLevel, txFunc func(context.Context, *Postgres) error) (err error) {
	if ok, _ := p.InTransaction(); ok {
		return errors.New("a DB Transact function was called on a DB already in a transaction")
	}

	mt := time.Now()
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.transact",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres_func", "transact"),
		}
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				newAttrs = append(newAttrs, attribute.String("postgres_err_code", code))
				span.SetAttributes(
					attribute.String("pg.errCode", code),
				)
			}
		}
		if errRecord := p.recordMetrics(ctx, mt, newAttrs); errRecord != nil {
			err = errors.Join(err, errRecord)
		}
		span.End()
	}()

	tx, beginErr := p.beginTx(spanCtx, iso)
	if beginErr != nil {
		err = beginErr
		return
	}
	// After we create the transaction object, the transaction object need to be closed by either commit or rollback
	// the query. So its better to use defer to ensure this.
	defer func() {
		if err == nil {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			rollbackErr = fmt.Errorf("failed to rollback: %w", rollbackErr)
			// Rollback error is a different error, join the error with the actual error.
			err = errors.Join(err, rollbackErr)
		}
	}()

	// Set the transaction span attributes including all metadata and informations. The downside of this is, the postgres.transact
	// span won't have any metadata inside it if something happen in beginTx.
	span.SetAttributes(tx.SpanAttributes()...)

	// Create a new copy of Postgres and add the transaction object inside the object. This will make InTransaction() check to be true.
	newPG := &Postgres{
		config:      p.config,
		closed:      p.closed,
		searchPath:  p.searchPath,
		db:          p.db,
		pgx:         p.pgx,
		tx:          tx,
		tracer:      p.tracer,
		meter:       p.meter,
		txIso:       iso,
		metricsName: p.metricsName,
	}
	err = txFunc(spanCtx, newPG)
	if err != nil {
		return
	}
	err = tx.Commit()
	if err != nil {
		err = fmt.Errorf("failed to commit: %w", err)
		return
	}
	return
}

func (p *Postgres) Prepare(ctx context.Context, query string) (sc *StmtCompat, err error) {
	mt := time.Now()
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.prepare",
		trace.WithSpanKind(trace.SpanKindInternal),
	)

	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres.func", "prepare"),
		}
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				newAttrs = append(newAttrs, attribute.String("postgres.err_code", code))
			}
		}
		if errRecord := p.recordMetrics(ctx, mt, newAttrs); errRecord != nil {
			err = errors.Join(err, errRecord)
		}
		span.SetAttributes(newAttrs...)
		span.End()
	}()

	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
		return p.tx.Prepare(ctx, query)
	}

	attrs := p.config.TracerConfig.traceAttributesFromContext(ctx, query)
	span.SetAttributes(attrs...)
	if p.pgx != nil {
		return &StmtCompat{
			sql:       query,
			pgxdb:     p.pgx,
			ctx:       spanCtx,
			tracer:    p.tracer,
			spanAttrs: attrs,
		}, nil
	}
	var stmt *sql.Stmt
	stmt, err = p.db.PrepareContext(spanCtx, query)
	if err != nil {
		return nil, err
	}
	return &StmtCompat{
		stmt:      stmt,
		ctx:       spanCtx,
		tracer:    p.tracer,
		spanAttrs: attrs,
	}, nil
}

func (p *Postgres) Ping(ctx context.Context) (err error) {
	mt := time.Now()
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.ping",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, "")...)

	defer func() {
		newAttrs := []attribute.KeyValue{
			attribute.String("postgres.func", "ping"),
		}
		if err != nil {
			var code string
			code, err = tryErrToPostgresError(err, p.IsPgx())
			span.SetStatus(codes.Error, err.Error())
			if code != "" {
				newAttrs = append(newAttrs, attribute.String("postgres.err_code", code))
			}
		}
		if errRecord := p.recordMetrics(ctx, mt, newAttrs); errRecord != nil {
			err = errors.Join(err, errRecord)
		}
		span.SetAttributes(newAttrs...)
		span.End()
	}()
	if p.pgx != nil {
		err = p.pgx.Ping(spanCtx)
		return
	}
	err = p.db.PingContext(spanCtx)
	return
}

// setDefaultSearchPath sets the default schema for the current connection.
func (p *Postgres) setDefaultSearchPath(ctx context.Context, schemaName string) error {
	query := fmt.Sprintf("SET search_path TO %s;", schemaName)
	_, err := p.Exec(ctx, query)
	if err != nil {
		return err
	}
	p.searchPathMu.Lock()
	p.searchPath = schemaName
	p.searchPathMu.Unlock()
	return nil
}

// SearchPath returns list of search path.
func (p *Postgres) SearchPath() []string {
	p.searchPathMu.RLock()
	paths := strings.Split(p.searchPath, ",")
	p.searchPathMu.RUnlock()
	return paths
}

func (p *Postgres) DefaultSearchPath() string {
	return p.SearchPath()[0]
}

func (p *Postgres) Close() (err error) {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}

	defer func() {
		// Cancel the monitoring goroutine and exit.
		p.cancelMonitorFn()
		if err != nil {
			p.closeMu.Unlock()
			return
		}
		p.closed = true
		p.closeMu.Unlock()
	}()

	if p.pgx != nil {
		p.pgx.Close()
		return
	}
	err = p.db.Close()
	return
}

// Sometimes other libraries require us to use the stdlib database. So we provide a function to do so.
func (p *Postgres) StdlibDB() *sql.DB {
	if p.db != nil {
		return p.db
	}
	// The pgx version will create a whole new connection instead of using the current one.
	copyConf := p.pgx.Config().Copy()
	return stdlib.OpenDB(*copyConf.ConnConfig)
}

// monitorPostgresStats creates a ticker loop and monitor the postgres database periodically via open telemetry.
func monitorPostgresStats(ctx context.Context, p *Postgres) error {
	tickerDuration := time.Second * 20
	ticker := time.NewTicker(tickerDuration)
	openConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.client.stats_max_open_conns")
	if err != nil {
		return err
	}
	idleConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.client.stats_idle_conns")
	if err != nil {
		return err
	}
	inUseConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.client.stats_in_use")
	if err != nil {
		return err
	}
	// Collect the stats for the first time so we don't have empty stats when the program starts.
	p.collectStats(
		ctx,
		idleConns,
		openConns,
		inUseConns,
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.collectStats(
				ctx,
				idleConns,
				openConns,
				inUseConns,
			)
		}
	}
}

func (p *Postgres) collectStats(ctx context.Context, idleConns, openConns, inUseConns metric.Int64Counter) {
	if p.pgx != nil {
		idleConns.Add(ctx, int64(p.pgx.Stat().IdleConns()))
		openConns.Add(ctx, int64(p.pgx.Stat().MaxConns()))
		inUseConns.Add(ctx, p.pgx.Stat().AcquireCount())
	} else {
		idleConns.Add(ctx, int64(p.db.Stats().Idle))
		openConns.Add(ctx, int64(p.db.Stats().MaxOpenConnections))
		inUseConns.Add(ctx, int64(p.db.Stats().InUse))
	}
}

// WithMetrics records the duration of function execution inside the fn. The function will only records metrics if the metrics name is not empty
// otherwise it will return a nil error.
//
// The metrics function is designed to records metrics across queries wrapped inside the WithMetrics scope. It will automatically propagates the
// metrics name into other cloned Postgres object to ensure all operations within the WithMetrics function has the same metrics name. The same metricsName
// across all operations are intended to ensure the users to have useful information across their executions. Do consider this case:
//
/* Transact (
//	Query
//	Query
//	Exec
*/
// )
//
// From above example, the package will produce four(4) histogram metrics:
//
// Operations
//    ^
//    |  <========================= 1. Transact ========================>
//    |    <==== 2. Query ====>
//    |                         <==== 3. Query ====>
//    |                                              <==== 4. Exec ====>
//    |
//    |-------------------------------------------------------------------> Time
//
// And from these four(4) metrics you will find that all "metrics_name" will be the same, so you can easily find all the metrics that have the same name
// to know more about on how the actual execution is being made.
func (p *Postgres) WithMetrics(ctx context.Context, name string, fn func(context.Context, *Postgres) error) (err error) {
	if name == "" {
		return errors.New("name cannot be empty to collect metrics")
	}
	metricsName := name
	// If the current postgres object metrics name is not emptym, then we should append the current metrics name with the upcoming metrics name.
	// So if the previous object have name of transactLedger and the upcoming metrics is createMovement, then the name will be transactLedger.createMovement.
	if p.metricsName != "" {
		metricsName = p.metricsName + "." + metricsName
	}
	pg := &Postgres{
		closed:      p.closed,
		searchPath:  p.searchPath,
		db:          p.db,
		pgx:         p.pgx,
		tx:          p.tx,
		tracer:      p.tracer,
		meter:       p.meter,
		txIso:       p.txIso,
		metricsName: metricsName,
	}
	return fn(ctx, pg)
}

func (p *Postgres) recordMetrics(ctx context.Context, t time.Time, attributes []attribute.KeyValue) error {
	// Without the metrics name we will produce a metrics across all queries and it will be less useful.
	// So we will only allow metrics recording when the metrics name is available, it means the user is aware
	// of what metrics they want to record.
	if p.metricsName == "" {
		return nil
	}
	attrs := p.meter.metricAttributesFromContext(ctx)
	if len(attributes) > 0 {
		attrs = append(attrs, attributes...)
	}
	// Set the operation name as one of the label for the metrics. The cardinality of the metrics will be as much
	// as the number of metrics name.
	attrs = append(attrs, attribute.String("execution.name", p.metricsName))

	histogram, err := p.meter.Meter.Float64Histogram("postgres.query_execution")
	if err != nil {
		return err
	}
	histogram.Record(ctx, time.Since(t).Seconds(), metric.WithAttributes(attrs...))
	return nil
}

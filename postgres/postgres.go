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
	"go.opentelemetry.io/otel/trace"
)

type Postgres struct {
	config ConnectConfig
	// tracer stores the pointer to the tracer configuration as we pass the tracer configuration everywhere.
	// We don't use pointer of the TracerConfig inside the ConnectConfig because the configuration can be copied
	// outside of the postgres package.
	//
	// As this configuration will be passed to transactions, etc. Please be awware to not change the values as
	// it will introduce a race.
	tracer *TracerConfig

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
	closeMu sync.Mutex
	closed  bool
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
	return p.config
}

// NewConfigFromDSN creates a new connect configuration from postgresql data source name.
func NewConfigFromDSN(dsn string) (ConnectConfig, error) {
	pgDSN, err := ParseDSN(dsn)
	if err != nil {
		return ConnectConfig{}, err
	}
	return ConnectConfig{
		Driver:   "pgx",
		Username: pgDSN.Username,
		Password: pgDSN.Password,
		Host:     pgDSN.Host,
		Port:     pgDSN.Port,
		SSLMode:  pgDSN.SSLMode,
	}, nil
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
		config:          config,
		tracer:          &config.TracerConfig,
		db:              db,
		pgx:             pgxdb,
		searchPath:      config.SearchPath,
		cancelMonitorFn: cancel,
	}
	// Start the monitoring goroutine if monitor configuration is on, we want to monitor the number of connections
	// and the general stats of the postgres object.
	if config.MeterConfig.MonitorInterval > 0 {
		go monitorPostgresStats(monitorCtx, p)
	}
	return p, nil
}

func (p *Postgres) Query(ctx context.Context, query string, params ...any) (*RowsCompat, error) {
	return p.query(ctx, query, params...)
}

func (p *Postgres) query(ctx context.Context, query string, params ...any) (rc *RowsCompat, err error) {
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.query",
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

func (p *Postgres) QueryRow(ctx context.Context, query string, params ...any) *RowCompat {
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.queryRow",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if p.tx != nil {
		span.SetAttributes(p.tx.SpanAttributes()...)
		return p.tx.QueryRow(spanCtx, query, params...)
	}

	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, query, params...)...)
	if p.pgx != nil {
		row := p.pgx.QueryRow(spanCtx, query, params...)
		return &RowCompat{pgxRow: row}
	}
	row := p.db.QueryRowContext(spanCtx, query, params...)
	return &RowCompat{row: row}
}

func (p *Postgres) Exec(ctx context.Context, query string, params ...any) (ec *ExecResultCompat, err error) {
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.exec",
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

	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.transact",
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
		config:     p.config,
		closed:     p.closed,
		searchPath: p.searchPath,
		db:         p.db,
		pgx:        p.pgx,
		tx:         tx,
		tracer:     p.tracer,
		txIso:      iso,
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
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.prepare",
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
	spanCtx, span := p.tracer.Tracer.Start(
		ctx,
		"postgres.ping",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	span.SetAttributes(p.config.TracerConfig.traceAttributesFromContext(ctx, "")...)
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
	ticker := time.NewTicker(p.config.MeterConfig.MonitorInterval)
	openConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.stats.max_open_conns")
	if err != nil {
		return err
	}
	idleConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.stats.idle_conns")
	if err != nil {
		return err
	}
	inUseConns, err := p.config.MeterConfig.Meter.Int64Counter("postgres.stats.in_use")
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
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
	}
}

package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/studio-asd/pkg/postgres"
	"go.opentelemetry.io/otel"
)

var (
	errPostgresDSNEmpty = errors.New("postgres dsn cannot be empty")
	errInvalidDSN       = errors.New("invalid postgres dsn")
)

const (
	defaultPostgresConnMaxLifetime    = time.Minute * 5
	defaultPostgresConnMaxIdleTime    = time.Minute
	defaultPostgresMaxOpenConnections = 20
	defaultPostgresMaxIdleConnections = 10
	defaultPostgresRetry              = 1
	defaultPostgresRetryDelay         = time.Second * 3
)

// PostgresResource provide more than one Postgres connections to provide primary and secondary connection
// to the Postgres database. Usually, there are two kinds of connections, primary and secondary that used for
// different purposes. Primary connection primarily used for 'write' and secondary connection primarily used for
// 'read' operations.
//
// Please use Primary() and Secondary() method to safely retrieve the connection from the pool/array.
type PostgresResource []*postgres.Postgres

// Primary returns the primary connection to the user.
func (pr PostgresResource) Primary() *postgres.Postgres {
	if len(pr) > 0 {
		return pr[0]
	}
	return nil
}

// Primary returns the secondary connection to the user. If the secondary is not exist, then the
// primary will be returned to the user.
func (pr PostgresResource) Secondary() *postgres.Postgres {
	if len(pr) > 1 {
		return pr[1]
	}
	// If we don't have a secondary database, then always return the primary one.
	return pr[0]
}

// postgresResources keeps all the PostgreSQL resources with name of the database as the key.
type postgresResources struct {
	mu sync.Mutex
	db map[string]PostgresResource
}

// newPostgresResources creates new postgres resources object.
func newpostgresResources() *postgresResources {
	return &postgresResources{
		db: make(map[string]PostgresResource),
	}
}

func (sr *postgresResources) setPostgres(name string, dbs []*postgres.Postgres) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.db[name] = dbs
}

// GetPostgres retrieve a PostgreSQL database based on its name.
func (sr *postgresResources) GetPostgres(name string) (PostgresResource, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	db, ok := sr.db[name]
	if !ok {
		return nil, fmt.Errorf("postgres with name %s does not exist", name)
	}
	return db, nil
}

// close closes all resources of all PostgreSQL resources.
func (sr *postgresResources) close() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	var err error
	for _, db := range sr.db {
		primaryErr := db.Primary().Close()
		if primaryErr != nil {
			err = errors.Join(err, primaryErr)
		}
		secondaryErr := db.Secondary().Close()
		if secondaryErr != nil {
			err = errors.Join(err, secondaryErr)
		}
	}
	return err
}

type PostgresOverrideableConfig struct {
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime Duration `yaml:"conn_max_idle-time"`
	MaxOpenConns    int      `yaml:"max_open_conns"`
	MaxIdleConns    int      `yaml:"max_idle_conns"`
	MaxRetry        int      `yaml:"max_retry"`
	RetryDelay      Duration `yaml:"retry_delay"`
	MonitorStats    bool     `yaml:"monitor_stats"`
}

func (soc *PostgresOverrideableConfig) SetDefault() {
	if soc.ConnMaxLifetime == 0 {
		soc.ConnMaxLifetime = Duration(defaultPostgresConnMaxLifetime)
	}
	if soc.ConnMaxIdleTime == 0 {
		soc.ConnMaxIdleTime = Duration(defaultPostgresConnMaxIdleTime)
	}
	if soc.MaxOpenConns == 0 {
		soc.MaxOpenConns = defaultPostgresMaxOpenConnections
	}
	if soc.MaxIdleConns == 0 {
		soc.MaxIdleConns = defaultPostgresMaxIdleConnections
	}
	if soc.MaxRetry == 0 {
		soc.MaxRetry = defaultPostgresRetry
	}
	if soc.RetryDelay == 0 {
		soc.RetryDelay = Duration(defaultPostgresRetryDelay)
	}
}

func (soc *PostgresOverrideableConfig) OverrideValue(val PostgresOverrideableConfig) {
	if soc.ConnMaxLifetime == 0 {
		soc.ConnMaxLifetime = val.ConnMaxLifetime
	}
	if soc.ConnMaxIdleTime == 0 {
		soc.ConnMaxIdleTime = val.ConnMaxIdleTime
	}
	if soc.MaxOpenConns == 0 {
		soc.MaxOpenConns = val.MaxOpenConns
	}
	if soc.MaxIdleConns == 0 {
		soc.MaxIdleConns = val.MaxIdleConns
	}
	if soc.MaxRetry == 0 {
		soc.MaxRetry = val.MaxRetry
	}
	if soc.RetryDelay == 0 {
		soc.RetryDelay = val.RetryDelay
	}
	// Only change the value to true if the parent value is true and child value is false.
	if soc.MonitorStats != val.MonitorStats && val.MonitorStats {
		soc.MonitorStats = val.MonitorStats
	}
}

type PostgresResourcesConfig struct {
	logger                     *slog.Logger
	PostgresConnections        []PostgresConnConfig `yaml:"connects"`
	PostgresOverrideableConfig `yaml:",inline"`
}

func (sc *PostgresResourcesConfig) Validate() error {
	sc.PostgresOverrideableConfig.SetDefault()
	for idx := range sc.PostgresConnections {
		if err := sc.PostgresConnections[idx].Validate(sc.PostgresOverrideableConfig); err != nil {
			return err
		}
	}
	return nil
}

func (sc *PostgresResourcesConfig) connect(ctx context.Context) (*postgresResources, error) {
	resources := newpostgresResources()
	for _, conn := range sc.PostgresConnections {
		primaryDSN, err := postgres.ParseDSN(conn.PrimaryDB.DSN)
		if err != nil {
			return nil, err
		}
		attrs := []slog.Attr{
			slog.String("db.name", conn.Name),
			slog.String("db.driver", conn.Driver),
			slog.String("db.primary_dsn", primaryDSN.SafeURL()),
		}

		sc.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][postgres] connecting to PostgreSQL database",
			attrs...,
		)
		conns, err := conn.Connect(ctx)
		if err != nil {
			sc.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][postgres] failed to connect to PostgreSQL database",
				attrs...,
			)
			return nil, err
		}
		resources.setPostgres(conn.Name, conns)
	}
	return resources, nil
}

type PostgresConnConfig struct {
	Name                       string         `yaml:"name"`
	Driver                     string         `yaml:"driver"`
	PrimaryDB                  PostgresConfig `yaml:"primary"`
	SecondaryDB                PostgresConfig `yaml:"secondary"`
	PostgresOverrideableConfig `yaml:",inline"`
}

type PostgresConfig struct {
	DSN                        string `yaml:"dsn"`
	PostgresOverrideableConfig `yaml:",inline"`
}

func (sdb *PostgresConfig) Validate(val PostgresOverrideableConfig) error {
	if sdb.DSN == "" {
		return errPostgresDSNEmpty
	}
	if _, err := postgres.ParseDSN(sdb.DSN); err != nil {
		return fmt.Errorf("%w: %w", errInvalidDSN, err)
	}
	sdb.OverrideValue(val)
	return nil
}

func (scc *PostgresConnConfig) Validate(val PostgresOverrideableConfig) error {
	if scc.Name == "" {
		return errors.New("resource: Postgres database name cannot be empty")
	}
	scc.OverrideValue(val)

	if err := scc.PrimaryDB.Validate(scc.PostgresOverrideableConfig); err != nil {
		return fmt.Errorf("error when validating primary db %s with error %w", scc.Name, err)
	}
	// We allow the secondary database dsn to be empty. This means we will also use the
	// primary as secondary or read source.
	if err := scc.SecondaryDB.Validate(scc.PostgresOverrideableConfig); err != nil && !errors.Is(err, errPostgresDSNEmpty) {
		return fmt.Errorf("error when validating secondary db %s with error %w", scc.Name, err)
	}
	return nil
}

func (scc *PostgresConnConfig) Connect(ctx context.Context) ([]*postgres.Postgres, error) {
	if scc.PrimaryDB.DSN == "" {
		return nil, errors.New("postgres primary db configuration cannot be empty")
	}
	values, err := postgres.ParseDSN(scc.PrimaryDB.DSN)
	if err != nil {
		return nil, err
	}

	var pg []*postgres.Postgres
	primaryPG, err := postgres.Connect(
		ctx,
		postgres.ConnectConfig{
			Driver:          "pgx",
			Username:        values.Username,
			Password:        values.Password,
			Host:            values.Host,
			Port:            values.Port,
			DBName:          values.DatabaseName,
			SSLMode:         values.SSLMode,
			MaxOpenConns:    scc.PrimaryDB.MaxOpenConns,
			ConnMaxIdletime: time.Duration(scc.PrimaryDB.ConnMaxIdleTime),
			ConnMaxLifetime: time.Duration(scc.PrimaryDB.ConnMaxLifetime),
			TracerConfig: postgres.TracerConfig{
				Tracer: otel.GetTracerProvider().Tracer("postgres"),
			},
			MeterConfig: postgres.MeterConfig{
				Meter:        otel.GetMeterProvider().Meter("postgres"),
				MonitorStats: scc.MonitorStats,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	if err := primaryPG.Ping(ctx); err != nil {
		return nil, err
	}

	pg = append(pg, primaryPG)
	// If we don't have the secondary postgres connection, then we should just return.
	if scc.SecondaryDB.DSN == "" {
		return pg, nil
	}

	values, err = postgres.ParseDSN(scc.SecondaryDB.DSN)
	if err != nil {
		return nil, err
	}
	secondaryPG, err := postgres.Connect(
		context.Background(),
		postgres.ConnectConfig{
			Driver:          "pgx",
			Username:        values.Username,
			Password:        values.Password,
			Host:            values.Host,
			Port:            values.Port,
			DBName:          values.DatabaseName,
			SSLMode:         values.SSLMode,
			MaxOpenConns:    scc.SecondaryDB.MaxOpenConns,
			ConnMaxIdletime: time.Duration(scc.SecondaryDB.ConnMaxIdleTime),
			ConnMaxLifetime: time.Duration(scc.SecondaryDB.ConnMaxLifetime),
		},
	)
	if err != nil {
		return nil, err
	}
	if err := secondaryPG.Ping(ctx); err != nil {
		return nil, err
	}

	pg = append(pg, secondaryPG)
	return pg, nil
}

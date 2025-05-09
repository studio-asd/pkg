package postgres

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/studio-asd/pkg/postgres"
	"github.com/studio-asd/pkg/resources"
	"github.com/studio-asd/pkg/srun"
)

var _ resources.PostgresPackage = (*PostgresLib)(nil)

// PostgresDB provide more than one Postgres connections to provide primary and secondary connection
// to the Postgres database. Usually, there are two kinds of connections, primary and secondary that used for
// different purposes. Primary connection primarily used for 'write' and secondary connection primarily used for
// 'read' operations.
//
// Please use Primary() and Secondary() method to safely retrieve the connection from the pool/array.
type PostgresDB []*postgres.Postgres

// Primary returns the primary connection to the user.
func (pr PostgresDB) Primary() *postgres.Postgres {
	if len(pr) > 0 {
		return pr[0]
	}
	return nil
}

// Primary returns the secondary connection to the user. If the secondary is not exist, then the
// primary will be returned to the user.
func (pr PostgresDB) Secondary() *postgres.Postgres {
	if len(pr) > 1 {
		return pr[1]
	}
	// If we don't have a secondary database, then always return the primary one.
	return pr[0]
}

// Close closes both primary and secondary postgresql connections.
func (pr PostgresDB) Close(ctx context.Context) error {
	var errs error
	for _, p := range pr {
		err := p.Close()
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func init() {
	lib := &PostgresLib{
		conns: make(map[string]PostgresDB),
	}
	resources.RegisterPackage(lib)
}

type PostgresLib struct {
	logger *slog.Logger
	conns  map[string]PostgresDB
}

func (lib *PostgresLib) SetLogger(logger *slog.Logger) {
	lib.logger = logger
}

func (lib *PostgresLib) Init(ctx srun.Context) error {
	return nil
}

func (lib *PostgresLib) Connect(ctx context.Context, config *resources.PostgresResourcesConfig) (map[string]any, error) {
	pg := make(map[string]any)
	for _, connConfig := range config.PostgresConnections {
		if connConfig.PrimaryDB.DSN == "" {
			return nil, errors.New("primary database DSN is empty")
		}
		var conns []*postgres.Postgres

		primaryDSN, err := postgres.ParseDSN(connConfig.PrimaryDB.DSN)
		if err != nil {
			return nil, err
		}
		attrs := []slog.Attr{
			slog.String("db.name", connConfig.Name),
			slog.String("db.driver", connConfig.Driver),
			slog.String("db.primary_dsn", primaryDSN.SafeURL()),
		}

		lib.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][postgres] connecting to PostgreSQL database",
			attrs...,
		)

		primaryConn, err := connect(ctx, connConfig.PrimaryDB)
		if err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
			lib.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][postgres] failed to connect to PostgreSQL database",
				attrs...,
			)
			return nil, err
		}
		conns = append(conns, primaryConn)

		if connConfig.SecondaryDB.DSN != "" {
			secondaryDSN, err := postgres.ParseDSN(connConfig.SecondaryDB.DSN)
			if err != nil {
				return nil, err
			}
			attrs := []slog.Attr{
				slog.String("db.name", connConfig.Name),
				slog.String("db.driver", connConfig.Driver),
				slog.String("db.secondary_dsn", secondaryDSN.SafeURL()),
			}

			lib.logger.LogAttrs(
				ctx,
				slog.LevelInfo,
				"[resources][postgres] connecting to PostgreSQL database",
				attrs...,
			)
			secondaryConn, err := connect(ctx, connConfig.SecondaryDB)
			if err != nil {
				attrs = append(attrs, slog.String("error", err.Error()))
				lib.logger.LogAttrs(
					ctx,
					slog.LevelError,
					"[resources][postgres] failed to connect to PostgreSQL database",
					attrs...,
				)
				return nil, err
			}
			// Append the secondary connection to the list of connections.
			conns = append(conns, secondaryConn)
		}
		// Save both connections as PostgresDB so we can use them as primary and secondary database.
		pgConns := PostgresDB(conns)
		pg[connConfig.Name] = pgConns
		// Register the connections into the library itself because we need to close all connections later on.
		lib.conns[connConfig.Name] = pgConns
	}
	return pg, nil
}

func (lib *PostgresLib) Close(ctx context.Context) error {
	var errs error
	for name, conn := range lib.conns {
		lib.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][postgres] closing connections to PostgreSQL database",
			slog.String("db.name", name),
		)
		err := conn.Close(ctx)
		if err != nil {
			lib.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][postgres] failed to close connections to PostgreSQL database",
				slog.String("db.name", name),
				slog.String("error", err.Error()),
			)
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func connect(ctx context.Context, config resources.PostgresConfig) (*postgres.Postgres, error) {
	values, err := postgres.ParseDSN(config.DSN)
	if err != nil {
		return nil, err
	}

	pg, err := postgres.Connect(
		ctx,
		postgres.ConnectConfig{
			Driver:          "pgx",
			Username:        values.Username,
			Password:        values.Password,
			Host:            values.Host,
			Port:            values.Port,
			DBName:          values.DatabaseName,
			SSLMode:         values.SSLMode,
			MaxOpenConns:    config.MaxOpenConns,
			ConnMaxIdletime: time.Duration(config.ConnMaxIdleTime),
			ConnMaxLifetime: time.Duration(config.ConnMaxLifetime),
			TracerConfig: &postgres.TracerConfig{
				Tracer: otel.GetTracerProvider().Tracer("postgres"),
			},
			MeterConfig: &postgres.MeterConfig{
				Meter:        otel.GetMeterProvider().Meter("postgres"),
				MonitorStats: config.MonitorStats,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return pg, pg.Ping(ctx)
}

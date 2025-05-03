package postgres

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/studio-asd/pkg/postgres"
	"github.com/studio-asd/pkg/resources"
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

func init() {
	lib := &PostgresLib{}
	resources.RegisterPackage(lib)
}

type PostgresLib struct {
	logger *slog.Logger
}

func (lib *PostgresLib) Name() string {
	return "postgres"
}

func (lib *PostgresLib) SetLogger(logger *slog.Logger) {
	lib.logger = logger
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
		pg[connConfig.Name] = PostgresDB(conns)
	}
	return pg, nil
}

func (lib *PostgresLib) Close(ctx context.Context) error {
	return nil
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

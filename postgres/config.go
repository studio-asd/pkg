package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/albertwidi/pkg/instrumentation"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	// the default max connections inside postgres is 100(https://www.postgresql.org/docs/current/runtime-config-connection.html#GUC-MAX-CONNECTIONS).
	// So we might want to keep the number low, and increase them in the configuration if needed.
	defaultMaxOpenConns = 30
	// some proxies set the default max conn idle time to 60 seconds, so we will put the same number here.
	defaultConnMaxIdletime = time.Minute
	defaultConnMaxLifetime = time.Minute * 30.
)

// ConnectConfig stores the configuration to create a new connection to PostgreSQL database.
type ConnectConfig struct {
	Driver          string
	Username        string
	Password        string
	Host            string
	Port            string
	DBName          string
	SearchPath      string
	SSLMode         string
	MaxOpenConns    int
	ConnMaxIdletime time.Duration
	ConnMaxLifetime time.Duration
	TracerConfig    *TracerConfig
}

func (c *ConnectConfig) validate() error {
	if c.Username == "" {
		return errors.New("postgres: username cannot be empty")
	}
	if c.Password == "" {
		return errors.New("postgres: password cannot be empty")
	}
	if c.Host == "" {
		return errors.New("postgres: host cannot be empty")
	}
	if c.Port == "" {
		return errors.New("postgres: port cannot be empty")
	}
	if c.SearchPath == "" {
		c.SearchPath = "public"
	}

	switch c.Driver {
	case "postgres", "libpq", "pgx":
	default:
		if c.Driver == "" {
			c.Driver = "<empty>"
		}
		return fmt.Errorf("postgres: driver %s is not supported. Please choose: postgres, libpq, pgx", c.Driver)
	}
	// Normalize the driver from 'libpq' and other drivers, because we will only support 'postgres'.
	if c.Driver == "libpq" {
		c.Driver = "postgres"
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}

	// Overrides values.
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = defaultMaxOpenConns
	}
	if c.ConnMaxIdletime == 0 {
		c.ConnMaxIdletime = defaultConnMaxIdletime
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.TracerConfig == nil {
		c.TracerConfig = &TracerConfig{}
	}
	if err := c.TracerConfig.validate(); err != nil {
		return err
	}
	// Inject the information to the tracer configuration as we want to inject these information
	// when create spans.
	c.TracerConfig.host = c.Host
	c.TracerConfig.user = c.Username
	c.TracerConfig.database = c.DBName
	return nil
}

// DSN returns the PostgreSQL DSN.
//
//	For example: postgres://username:password@localhost:5432/mydb?sslmode=false.
func (c *ConnectConfig) DSN() (url string, dsn DSN, err error) {
	url = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", c.Username, c.Password, c.Host, c.Port, c.DBName, c.SSLMode)
	if c.SearchPath != "" {
		url = url + "&search_path=" + c.SearchPath
	}
	dsn, err = ParseDSN(url)
	return
}

type TracerConfig struct {
	// Tracer is the opentelemetry tracer to trace queries and operations inside the postgres package.
	// If the tracer value is nil, then noop tracer will be used and no trace will be recorded.
	Tracer trace.Tracer
	// RecordArgs is a flag to records the query arguments.
	RecordArgs bool

	// below configurations are injected to enrich the trace/span atrrbutes.
	host     string
	database string
	user     string
}

func (t *TracerConfig) validate() error {
	if t.Tracer == nil {
		t.Tracer = noop.NewTracerProvider().Tracer("noop")
	}
	return nil
}

// traceAttributes returns the trace attributes from query and the query arguments.
func (t *TracerConfig) traceAttributes(query string, args ...any) []attribute.KeyValue {
	// initial informations about the configuration and connection attributes.
	attrs := []attribute.KeyValue{
		attribute.String("pg.config.host", t.host),
		attribute.String("pg.config.database", t.database),
		attribute.String("pg.config.user", t.user),
	}
	if query != "" {
		attrs = append(attrs, attribute.String("postgres.query", query))
	}
	if t.RecordArgs {
		attrs = append(attrs, attribute.StringSlice("postgres.query_args", queryArgsToStringSlice(args)))
	}
	return attrs
}

// traceAttributesFromcontext returns the trace attributes from context, query and the query arguments.
func (t *TracerConfig) traceAttributesFromContext(ctx context.Context, query string, args ...any) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	b := instrumentation.BaggageFromContext(ctx)
	if !b.Empty() {
		attrs = append(attrs, attribute.String("request.id", b.RequestID))
		attrs = append(attrs, attribute.String("api.name", b.APIName))
		attrs = append(attrs, attribute.String("api.owner", b.APIOwner))
		if b.DebugID != "" {
			attrs = append(attrs, attribute.String("debug_id", b.DebugID))
		}
	}
	ta := t.traceAttributes(query, args...)
	if len(ta) > 0 {
		attrs = append(attrs, ta...)
	}
	return attrs
}

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
	defaultMaxOpenConns = 10
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

	switch c.Driver {
	case "postgres", "libpq", "pgx":
	default:
		return fmt.Errorf("postgres: driver %s is not supported", c.Driver)
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
	if c.TracerConfig == nil {
		c.TracerConfig = &TracerConfig{}
	}
	if err := c.TracerConfig.validate(); err != nil {
		return err
	}
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
}

func (t *TracerConfig) validate() error {
	if t.Tracer == nil {
		t.Tracer = noop.NewTracerProvider().Tracer("noop")
	}
	return nil
}

// traceAttributes returns the trace attributes from query and the query arguments.
func (t *TracerConfig) traceAttributes(query string, args ...any) []attribute.KeyValue {
	var attrs []attribute.KeyValue
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
		attrs = append(attrs, attribute.String("request_id", b.RequestID))
		attrs = append(attrs, attribute.String("api_name", b.APIName))
		attrs = append(attrs, attribute.String("api_owner", b.APIOwner))
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

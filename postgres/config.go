package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/studio-asd/pkg/instrumentation"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
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
	// Driver is the Go driver that will be used to connect to PostgreSQL database. For example, libpq/pgx.
	Driver string
	// Username is the username to connect to the PostgreSQL database.
	Username string
	// Password is the password to connect to the PostgreSQL database.
	Password string
	// Host is the host address to connect to the PostgreSQL database. For example, localhost.
	Host string
	// Port is the port to connecto to the PostgreSQL database. For example, 5432.
	Port   string
	DBName string
	// SearchPath is the default search path/schema path for PostgreSQL. The search path is being set in the dsn when
	// connecting to postgresql.
	SearchPath      string
	SSLMode         string
	MaxOpenConns    int
	ConnMaxIdletime time.Duration
	ConnMaxLifetime time.Duration
	// TracerConfig holds the tracer configuration along with otel tracer inside it.
	TracerConfig *TracerConfig
	// MeterConfig holds the meter configuration along with otel meter inside it.
	MeterConfig *MeterConfig
}

func (c *ConnectConfig) validate() error {
	if c.Username == "" {
		return errors.New("postgres: username cannot be empty")
	}
	if c.Password == "" {
		return errors.New("postgres: password cannot be empty")
	}
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == "" {
		c.Port = "5432"
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
	if c.MeterConfig == nil {
		c.MeterConfig = &MeterConfig{}
	}
	if err := c.MeterConfig.validate(); err != nil {
		return err
	}
	// Inject the information to both tracer and meter configuration as we want to inject these information
	// when creating spans and metrics.
	// Tracer.
	c.TracerConfig.host = c.Host
	c.TracerConfig.user = c.Username
	c.TracerConfig.database = c.DBName
	// Meter.
	c.MeterConfig.host = c.Host
	c.MeterConfig.user = c.Username
	c.MeterConfig.database = c.DBName
	return nil
}

// DSN returns the PostgreSQL DSN.
//
//	For example: postgres://username:password@localhost:5432/mydb?sslmode=false.
func (c *ConnectConfig) DSN() (dsn DSN, err error) {
	dsn, err = ParseDSN(buildPostgresURL(c.Username, c.Password, c.Host, c.Port, c.DBName, c.SSLMode, c.SearchPath))
	return
}

func buildPostgresURL(username, password, host, port, dbName, sslMode, searchPath string) string {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", username, password, host, port, dbName, sslMode)
	if searchPath != "" {
		url = url + "&search_path=" + searchPath
	}
	return url
}

type TracerConfig struct {
	// Tracer is the opentelemetry tracer to trace queries and operations inside the postgres package.
	// If the tracer value is nil, then noop tracer will be used and no trace will be recorded.
	Tracer trace.Tracer
	// RecordArgs is a flag to records the query arguments.
	RecordArgs bool
	// DefaultAttributes is the default attributes that will get appended to all traces inside the package.
	DefaultAttributes []attribute.KeyValue

	// below configurations are injected to enrich the trace/span atrrbutes.
	host     string
	database string
	user     string
}

func (t *TracerConfig) validate() error {
	if t.Tracer == nil {
		t.Tracer = otel.GetTracerProvider().Tracer("postgres")
	}
	return nil
}

// traceAttributes returns the trace attributes from query and the query arguments.
func (t *TracerConfig) traceAttributes(query string, args ...any) []attribute.KeyValue {
	// initial informations about the configuration and connection attributes.
	attrs := []attribute.KeyValue{
		attribute.String("postgres.config.host", t.host),
		attribute.String("postgres.config.database", t.database),
		attribute.String("postgres.config.user", t.user),
	}
	// Append the default attributes if we have it.
	if len(t.DefaultAttributes) > 0 {
		attrs = append(attrs, t.DefaultAttributes...)
	}
	if query != "" {
		attrs = append(attrs, attribute.String("postgres.query_value", query))
	}
	if t.RecordArgs {
		attrs = append(attrs, attribute.StringSlice("postgres.query_args", queryArgsToStringSlice(args)))
	}
	return attrs
}

// traceAttributesFromcontext returns the trace attributes from context, query and the query arguments.
func (t *TracerConfig) traceAttributesFromContext(ctx context.Context, query string, args ...any) []attribute.KeyValue {
	attrs := instrumentation.BaggageFromContext(ctx).ToOpenTelemetryAttributes()
	ta := t.traceAttributes(query, args...)
	if len(ta) > 0 {
		attrs = append(attrs, ta...)
	}
	return attrs
}

type MeterConfig struct {
	// MonitorStats enable PostgreSQL stats monitoring in the background. The stats will be collected for every 30 seconds.
	MonitorStats      bool
	Meter             metric.Meter
	DefaultAttributes []attribute.KeyValue
	//
	// below configurations are injected to enrich the meter atrrbutes.
	host     string
	database string
	user     string
}

func (m *MeterConfig) validate() error {
	if m.Meter == nil {
		m.Meter = otel.GetMeterProvider().Meter("postgres")
	}
	return nil
}

func (m *MeterConfig) metricsAttributes() []attribute.KeyValue {
	// initial informations about the configuration and connection attributes.
	attrs := []attribute.KeyValue{
		attribute.String("postgres.config.host", m.host),
		attribute.String("postgres.config.database", m.database),
		attribute.String("postgres.config.user", m.user),
	}
	// Append the default attributes if we have it.
	if len(m.DefaultAttributes) > 0 {
		attrs = append(attrs, m.DefaultAttributes...)
	}
	return attrs
}

// traceAttributesFromcontext returns the trace attributes from context, query and the query arguments.
func (m *MeterConfig) metricAttributesFromContext(ctx context.Context) []attribute.KeyValue {
	attrs := instrumentation.BaggageFromContext(ctx).ToOpenTelemetryAttributesForMetrics()
	ta := m.metricsAttributes()
	if len(ta) > 0 {
		attrs = append(attrs, ta...)
	}
	return attrs
}

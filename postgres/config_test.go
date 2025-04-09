package postgres

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/otel"
)

func TestConfigvalidate(t *testing.T) {
	t.Parallel()

	t.Run("must_have", func(t *testing.T) {
		t.Parallel()

		c := ConnectConfig{}
		if err := c.validate(); err.Error() != "postgres: username cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Username = "test"
		if err := c.validate(); err.Error() != "postgres: password cannot be empty" {
			t.Fatal("expecting username error")
		}

		c.Password = "test"
		if err := c.validate(); err.Error() != "postgres: driver <empty> is not supported. Please choose: postgres, libpq, pgx" {
			t.Fatal("expecting driver error")
		}

		c.Driver = "pgx"
		if err := c.validate(); err != nil {
			t.Fatal(err)
		}

		expect := ConnectConfig{
			Username:        "test",
			Password:        "test",
			Driver:          "pgx",
			Host:            "127.0.0.1",
			Port:            "5432",
			SearchPath:      "public",
			SSLMode:         "disable",
			MaxOpenConns:    30,
			ConnMaxIdletime: defaultConnMaxIdletime,
			ConnMaxLifetime: defaultConnMaxLifetime,
			TracerConfig: &TracerConfig{
				Tracer: otel.GetTracerProvider().Tracer("postgres"),
			},
			MeterConfig: &MeterConfig{
				Meter: otel.GetMeterProvider().Meter("postgres"),
			},
		}
		ignoreTracer := cmpopts.IgnoreFields(TracerConfig{}, "Tracer")
		ignoreMeter := cmpopts.IgnoreFields(MeterConfig{}, "Meter")
		ignoreUnexported := cmpopts.IgnoreUnexported(TracerConfig{}, MeterConfig{})
		if diff := cmp.Diff(expect, c, ignoreTracer, ignoreMeter, ignoreUnexported); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})

	t.Run("override_values", func(t *testing.T) {
		t.Parallel()

		c := ConnectConfig{
			Username: "test",
			Password: "test",
			Host:     "localhost",
			Port:     "5432",
			Driver:   "pgx",
		}
		if err := c.validate(); err != nil {
			t.Fatal(err)
		}
		if c.MaxOpenConns != defaultMaxOpenConns {
			t.Fatalf("expecting max open conns %d but got %d", defaultMaxOpenConns, c.MaxOpenConns)
		}
		if c.ConnMaxLifetime != defaultConnMaxLifetime {
			t.Fatalf("expecting conn max lifetime %d but got %d", defaultConnMaxLifetime, c.ConnMaxLifetime)
		}
		if c.ConnMaxIdletime != defaultConnMaxIdletime {
			t.Fatalf("expecting max idle time %d but got %d", defaultConnMaxIdletime, c.ConnMaxIdletime)
		}

		tracerName := reflect.TypeOf(c.TracerConfig.Tracer).String()
		if tracerName != "*global.tracer" {
			t.Fatalf("expecting tracer %s but got %s", "*global.tracer", tracerName)
		}
	})
}

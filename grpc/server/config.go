package server

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	meternoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type Config struct {
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	GRPCGateway  GRPCGateway
	// Below configurations will automatically applied to the grpc gateway as well.
	Trace *TraceConfig
	Meter *MeterConfig
}

func (c Config) Validate() error {
	if c.Trace == nil {
		c.Trace = &TraceConfig{
			// Use a noop trace provider as the default trace configuration.
			Tracer: tracenoop.NewTracerProvider().Tracer("noop"),
		}
	}
	if err := c.Trace.Validate(); err != nil {
		return err
	}
	if c.Meter == nil {
		c.Meter = &MeterConfig{
			Meter: meternoop.NewMeterProvider().Meter("noop"),
		}
	}
	if err := c.Meter.Validate(); err != nil {
		return err
	}
	return nil
}

type TraceConfig struct {
	Tracer            trace.Tracer
	DefaultAttributes []attribute.KeyValue
}

func (t *TraceConfig) Validate() error {
	return nil
}

type MeterConfig struct {
	Meter             metric.Meter
	DefaultAttributes []attribute.KeyValue
}

func (m *MeterConfig) Validate() error {
	return nil
}

// GRPCGateway configurations enables grpc-gateway for json-protobuf translation.
// You can read more about it here: https://grpc-ecosystem.github.io/grpc-gateway/docs/.
type GRPCGateway struct {
	Address string
}

func (g GRPCGateway) Validate() error {
	return nil
}

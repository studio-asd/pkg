package server

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	meternoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	defaultReadTimeout  = time.Second * 30
	defaultWriteTimeout = time.Second * 20
)

type Config struct {
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// Below configurations will automatically applied to the grpc gateway as well.
	Trace *TraceConfig
	Meter *MeterConfig
}

func (c *Config) Validate() error {
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}

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

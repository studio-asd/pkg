package srun

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	meternoop "go.opentelemetry.io/otel/metric/noop"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type OTelTracerConfig struct {
	Disable bool
	// Below is a private configuration passed from the srun itself to provide several information
	// for the open-telemetry.
	serviceName    string
	serviceVersion string
}

// newOTelTracerService returns a function to trigger and starts open telemetry processes. The function returns a function to allow us to use
// the LongRunningTask so we can listen to the exit signal.
func newOTelTracerService(config OTelTracerConfig) (trace.Tracer, *LongRunningTask, error) {
	if config.Disable {
		return tracenoop.NewTracerProvider().Tracer("noop"), nil, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.serviceName),
			semconv.ServiceVersion(config.serviceVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, nil, err
	}

	provider := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(
			exporter,
			// The default timeout is 5s, but we want to be explicit about it.
			tracesdk.WithBatchTimeout(time.Second*5),
		),
		tracesdk.WithResource(res),
	)
	tracer := provider.Tracer(config.serviceName)

	fn := func(ctx Context) error {
		// Wait until the context is cancalled to shutdown the provider.
		<-ctx.Ctx.Done()

		ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		// Forcefully flush all registered spans before shutdown to ensure we are sending all traces.
		if err := provider.ForceFlush(ctxTimeout); err != nil {
			ctx.Logger.Error(
				"failed to flush otel-traces",
				slog.String("error", err.Error()),
			)
		}
		return provider.Shutdown(ctxTimeout)
	}
	task, err := NewLongRunningTask("otel-tracer-listener", fn)
	if err != nil {
		return nil, nil, err
	}
	return tracer, task, nil
}

type OtelMetricConfig struct {
	// Disable disables metrics collection via otel/noop package that essentially doing nothing.
	Disable bool
	// MeterName is the name of metric meter for otel meter provider.
	MeterName string
	// Below is a private configuration passed from the srun itself to provide several information
	// for the open-telemetry.
	serviceName    string
	serviceVersion string
}

// newOtelMetricsMeterAndProviderService returns open telemetry meter and provider so we can use them inside the runner and inject it to the Context.
// The function returns otel provider as LongRunningTask as we need to shut it down when the program stops to properly flush all metrics.
func newOtelMetricMeterAndProviderService(config OtelMetricConfig) (metric.Meter, *LongRunningTask, error) {
	if config.Disable {
		return meternoop.NewMeterProvider().Meter("noop"), nil, nil
	}
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, nil, err
	}
	provider := metricsdk.NewMeterProvider(
		metricsdk.WithReader(promExporter),
	)
	meter := provider.Meter(
		config.MeterName,
		metric.WithInstrumentationVersion(config.serviceVersion),
		metric.WithInstrumentationAttributes(
			attribute.String("app.name", config.serviceName),
			attribute.String("app.version", config.serviceVersion),
		),
	)

	providerTask, err := NewLongRunningTask("otel-metric-provider", func(ctx Context) error {
		// Wait until the context is cancalled to shutdown the provider.
		<-ctx.Ctx.Done()

		ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		// Forcefully flush all pending telemetry before shutdown to ensure we are sending all telemetries.
		if err := provider.ForceFlush(ctxTimeout); err != nil {
			ctx.Logger.Error(
				"failed to flush otel-telemetry",
				slog.String("error", err.Error()),
			)
		}
		return provider.Shutdown(ctxTimeout)
	})
	if err != nil {
		return nil, nil, err
	}
	return meter, providerTask, nil
}

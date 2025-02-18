package srun

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
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
	goVersion      string
}

// newOTelTracerService returns a function to trigger and starts open telemetry processes. The function returns a function to allow us to use
// the LongRunningTask so we can listen to the exit signal.
func newOTelTracerService(config OTelTracerConfig) (trace.Tracer, *LongRunningTask, error) {
	if config.Disable {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return tracenoop.NewTracerProvider().Tracer("noop"), nil, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.serviceName),
			semconv.ServiceVersion(config.serviceVersion),
			attribute.String("go.version", config.goVersion),
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
	// Set a global trace provider so it can be used elsewhere in the program.
	otel.SetTracerProvider(provider)
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
	goVersion      string
}

// newOtelMetricsMeterAndProviderService returns open telemetry meter and provider so we can use them inside the runner and inject it to the Context.
// The function returns otel provider as LongRunningTask as we need to shut it down when the program stops to properly flush all metrics.
func newOtelMetricMeterAndProviderService(config OtelMetricConfig) (metric.Meter, *LongRunningTask, error) {
	if config.Disable {
		otel.SetMeterProvider(meternoop.NewMeterProvider())
		return meternoop.NewMeterProvider().Meter("noop"), nil, nil
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.serviceName),
			semconv.ServiceVersion(config.serviceVersion),
			attribute.String("go_version", config.goVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	promExporter, err := prometheus.New()
	if err != nil {
		return nil, nil, err
	}
	provider := metricsdk.NewMeterProvider(
		metricsdk.WithReader(promExporter),
		metricsdk.WithResource(res),
	)
	// Set the meter provider so it can be used elsewhere in the program
	otel.SetMeterProvider(provider)
	meter := provider.Meter(
		config.MeterName,
	)

	providerTask, err := NewLongRunningTask("otel-metric-provider", func(ctx Context) error {
		// Start the runtime stats retrieval with resolution of ten(10) seconds.
		if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second * 10)); err != nil {
			return err
		}
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

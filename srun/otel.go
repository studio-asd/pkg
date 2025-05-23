package srun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
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
	Disable  bool
	Exporter OtelTracerExporter
	// Below is a private configuration passed from the srun itself to provide several information
	// for the open-telemetry.
	appName    string
	appVersion string
	goVersion  string
}

func (o *OTelTracerConfig) Validate() error {
	// If the tracing is disabled, then we should return early and don't bother with anything else.
	if o.Disable {
		return nil
	}
	if err := o.Exporter.Validate(); err != nil {
		return err
	}
	return nil
}

type OtelTracerExporter struct {
	GRPC *OtelTracerGRPCExporter
	HTTP *OtelTracerHTTPExporter
}

func (o *OtelTracerExporter) Validate() error {
	// Check the environment variable value and set the exporter based on the environment variable.
	exporter, ok := os.LookupEnv("SRUN_OTEL_TRACE_EXPORTER")
	if !ok {
		return nil
	}

	var (
		insecure bool
		err      error
	)
	endpoint := os.Getenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT")
	endpointInsecure := os.Getenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT_INSECURE")
	if endpointInsecure != "" {
		insecure, err = strconv.ParseBool(endpointInsecure)
		if err != nil {
			return fmt.Errorf("invalid value for SRUN_OTEL_TRACE_EXPORTER_ENDPOINT_INSECURE, need boolean value but got %s", endpointInsecure)
		}
	}

	switch exporter {
	case "http":
		o.HTTP = &OtelTracerHTTPExporter{
			Endpoint: endpoint,
			Insecure: insecure,
		}
	case "grpc":
		o.GRPC = &OtelTracerGRPCExporter{
			Endpoint: endpoint,
			Insecure: insecure,
		}
	}
	return nil
}

type OtelTracerGRPCExporter struct {
	Endpoint string
	Insecure bool
}

type OtelTracerHTTPExporter struct {
	Endpoint string
	URLPath  string
	Insecure bool
}

// newOTelTracerService returns a function to trigger and starts open telemetry processes. The function returns a function to allow us to use
// the LongRunningTask so we can listen to the exit signal.
func newOTelTracerService(config OTelTracerConfig) (trace.Tracer, *LongRunningTask, error) {
	if config.Disable {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return tracenoop.NewTracerProvider().Tracer("noop"), nil, nil
	}
	// For now, if there is no exporter then we will ignore all the configurations.
	if config.Exporter.HTTP == nil && config.Exporter.GRPC == nil {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return tracenoop.NewTracerProvider().Tracer("noop"), nil, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.appName),
			semconv.ServiceVersion(config.appVersion),
			attribute.String("go.version", config.goVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	var exporter tracesdk.SpanExporter
	if config.Exporter.HTTP != nil {
		var opts []otlptracehttp.Option
		if config.Exporter.HTTP.Endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(config.Exporter.HTTP.Endpoint))
		}
		if config.Exporter.HTTP.URLPath != "" {
			opts = append(opts, otlptracehttp.WithURLPath(config.Exporter.HTTP.URLPath))
		}
		if config.Exporter.HTTP.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(context.Background(), opts...)
		if err != nil {
			return nil, nil, err
		}
	}
	if config.Exporter.GRPC != nil {
		var opts []otlptracegrpc.Option
		if config.Exporter.GRPC.Endpoint != "" {
			opts = append(opts, otlptracegrpc.WithEndpoint(config.Exporter.GRPC.Endpoint))
		}
		if config.Exporter.GRPC.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(context.Background(), opts...)
		if err != nil {
			return nil, nil, err
		}
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
	tracer := provider.Tracer(config.appName)

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
		var err error
		if pErr := provider.Shutdown(ctxTimeout); pErr != nil {
			err = errors.Join(err, pErr)
		}
		if eErr := exporter.Shutdown(ctxTimeout); eErr != nil {
			err = errors.Join(err, eErr)
		}
		return err
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
	Exporter  OtelMetricExporter
	// Below is a private configuration passed from the srun itself to provide several information
	// for the open-telemetry.
	appName    string
	appVersion string
	goVersion  string
}

func (o *OtelMetricConfig) Validate() error {
	if err := o.Exporter.Validate(); err != nil {
		return err
	}
	return nil
}

type OtelMetricExporter struct {
	Prometheus *OtelMetricPrometheusExporter
	HTTP       *OtelMetricHTTPExporter
	GRPC       *OtelMetricGRPCExporter
}

func (o *OtelMetricExporter) Validate() error {
	var (
		insecure bool
		err      error
	)

	exporter := os.Getenv("SRUN_OTEL_METRIC_EXPORTER")
	endpoint := os.Getenv("SRUN_OTEL_METRIC_EXPORTER_ENDPOINT")
	endpointInsecure := os.Getenv("SRUN_OTEL_METRIC_EXPORTER_ENDPOINT_INSECURE")
	if endpointInsecure != "" {
		insecure, err = strconv.ParseBool(endpointInsecure)
		if err != nil {
			return fmt.Errorf("invalid value for SRUN_OTEL_METRIC_EXPORTER_ENDPOINT_INSECURE, need boolean value but got %s", endpointInsecure)
		}
	}

	switch exporter {
	case "prometheus":
		o.Prometheus = &OtelMetricPrometheusExporter{}
	case "http":
		o.HTTP = &OtelMetricHTTPExporter{
			Endpoint: endpoint,
			Insecure: insecure,
		}
	case "grpc":
		o.GRPC = &OtelMetricGRPCExporter{
			Endpoint: endpoint,
			Insecure: insecure,
		}
	// By default, use prometheus as the exporter.
	default:
		o.Prometheus = &OtelMetricPrometheusExporter{}
	}
	return nil
}

type OtelMetricPrometheusExporter struct{}

type OtelMetricHTTPExporter struct {
	Endpoint string
	Insecure bool
}

type OtelMetricGRPCExporter struct {
	Endpoint string
	Insecure bool
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
			semconv.ServiceName(config.appName),
			semconv.ServiceVersion(config.appVersion),
			attribute.String("go.version", config.goVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	var exporter metricsdk.Reader
	if config.Exporter.Prometheus != nil {
		exporter, err = prometheus.New()
		if err != nil {
			return nil, nil, err
		}
	}
	if config.Exporter.HTTP != nil {
		options := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(config.Exporter.HTTP.Endpoint),
		}
		if config.Exporter.HTTP.Insecure {
			options = append(options, otlpmetrichttp.WithInsecure())
		}
		httpExporter, err := otlpmetrichttp.New(
			context.Background(),
			options...,
		)
		if err != nil {
			return nil, nil, err
		}
		exporter = metricsdk.NewPeriodicReader(httpExporter)
	}
	if config.Exporter.GRPC != nil {
		options := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(config.Exporter.GRPC.Endpoint),
		}
		if config.Exporter.GRPC.Insecure {
			options = append(options, otlpmetricgrpc.WithInsecure())
		}
		grpcExporter, err := otlpmetricgrpc.New(
			context.Background(),
			options...,
		)
		if err != nil {
			return nil, nil, err
		}
		exporter = metricsdk.NewPeriodicReader(grpcExporter)
	}

	provider := metricsdk.NewMeterProvider(
		metricsdk.WithReader(exporter),
		metricsdk.WithResource(res),
	)
	// Set the meter provider so it can be used elsewhere in the program.
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

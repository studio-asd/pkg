package instrumentation

import "os"

var defaultTracer = os.Getenv("GOPKG_TRACER_DEFAULT")

const (
	Otel    = "opentelemetry"
	Datadog = "datadog"
)

// DefaultTracer returns the default tracer for the entire package.
func DefaultInstrumentation() string {
	if defaultTracer == "" {
		return Otel
	}
	return defaultTracer
}

package instrumentation

import "os"

var defaultTracer = os.Getenv("GOPKG_TRACER_DEFAULT")

const (
	// TraceOtel is the tracer instrumentation for open telemetry project.
	TracerOtel    = "opentelemetry"
	TracerDatadog = "datadog"
)

// DefaultTracer returns the default tracer for the entire package.
func DefaultTracer() string {
	if defaultTracer == "" {
		return TracerOtel
	}
	return defaultTracer
}

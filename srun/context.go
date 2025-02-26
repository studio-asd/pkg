package srun

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Context holds runner context including all objects that belong to the runner. For example we can pass logger and otel meter
// object via this context.
type Context struct {
	// RunnerAppName is the same with config.Name or the service name for the runner.
	RunnerAppName    string
	RunnerAppVersion string
	Ctx              context.Context
	Logger           *slog.Logger
	// Meter is open telemetry metric meter object to record metrics via open telemetry provider. The provider exports the metric
	// via prometheus exporter.
	//
	// You need to pass/inject the meter object to another function/struct to use this meter.
	Meter  metric.Meter
	Tracer trace.Tracer
	// HealthNotifier is the healthcheck notifier to notify the health check service about the current status of the service.
	//
	// Please NOTE that the notifier will always be nil for ServiceInitAware as we don't track the state of init aware service thus
	// letting them to blast notification doesn't seems meaningful.
	HealthNotifier *HealthcheckNotifier
	Flags          *Flags
}

// NewStateHelper returns a new state helper that can help the service to maintain its own state.
func (c *Context) NewStateHelper(name string) *StateHelper {
	return &StateHelper{
		serivceName: name,
	}
}

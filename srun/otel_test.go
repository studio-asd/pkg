package srun

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestOtelTracer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		config     OTelTracerConfig
		tracerType trace.Tracer
		nilTask    bool
		err        error
	}{
		{
			name: "default configuration",
			config: OTelTracerConfig{
				Exporter: OtelTracerExporter{
					HTTP: &OtelTracerHTTPExporter{},
				},
			},
			tracerType: nooptrace.Tracer{},
			nilTask:    false,
			err:        nil,
		},
		{
			name: "noop",
			config: OTelTracerConfig{
				Disable: true,
			},
			tracerType: nooptrace.Tracer{},
			nilTask:    true,
			err:        nil,
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tracer, task, err := newOTelTracerService(tt.config)
			if err != tt.err {
				t.Fatalf("expecting error %v but got %v", tt.err, err)
			}
			if (task == nil) != tt.nilTask {
				t.Fatalf("expecting %v but got %v", tt.nilTask, (task == nil))
			}

			// If we got the provided tracer from otel, then we cannot check the type because it is
			// an internal type. Otherwise, check the tracer type.
			if !tt.config.Disable {
				if tracer == nil {
					t.Fatal("tracer is nil")
				}
				return
			}

			tracerType := fmt.Sprintf("%T", tracer)
			expectTracerType := fmt.Sprintf("%T", tt.tracerType)
			if tracerType != expectTracerType {
				t.Fatalf("expecting tracer type %s but got %s", expectTracerType, tracerType)
			}
		})
	}
}

func TestOtelMeter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		config    OtelMetricConfig
		meterType metric.Meter
		nilTask   bool
		err       error
	}{
		{
			name:      "default configuration",
			config:    OtelMetricConfig{},
			meterType: noop.Meter{},
			nilTask:   false,
			err:       nil,
		},
		{
			name: "noop",
			config: OtelMetricConfig{
				Disable: true,
			},
			meterType: noop.Meter{},
			nilTask:   true,
			err:       nil,
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tracer, task, err := newOtelMetricMeterAndProviderService(tt.config)
			if err != tt.err {
				t.Fatalf("expecting error %v but got %v", tt.err, err)
			}
			if (task == nil) != tt.nilTask {
				t.Fatalf("expecting %v but got %v", tt.nilTask, (task == nil))
			}

			// If we got the provided meter from otel, then we cannot check the type because it is
			// an internal type. Otherwise, check the meter type.
			if !tt.config.Disable {
				if tracer == nil {
					t.Fatal("meter is nil")
				}
				return
			}

			meterType := fmt.Sprintf("%T", tracer)
			expectMeterType := fmt.Sprintf("%T", tt.meterType)
			if meterType != expectMeterType {
				t.Fatalf("expecting tracer type %s but got %s", expectMeterType, meterType)
			}
		})
	}
}

func TestOtelTracerConfigValidate(t *testing.T) {
	tests := []struct {
		name       string
		setenvFunc func(t *testing.T)
		config     OTelTracerConfig
		expect     OTelTracerConfig
		expectErr  bool
	}{
		{
			name: "disable, nil exporter",
			config: OTelTracerConfig{
				Disable: true,
			},
			expect: OTelTracerConfig{
				Disable: true,
			},
			expectErr: false,
		},
		{
			name: "disable, non-nil exporter",
			config: OTelTracerConfig{
				Disable: true,
				Exporter: OtelTracerExporter{
					GRPC: &OtelTracerGRPCExporter{},
					HTTP: &OtelTracerHTTPExporter{},
				},
			},
			expect: OTelTracerConfig{
				Disable: true,
			},
			expectErr: false,
		},
		{
			name: "enable, empty exporter",
			config: OTelTracerConfig{
				Disable: false,
			},
			expect: OTelTracerConfig{
				Disable: false,
			},
			expectErr: false,
		},
		{
			name: "enable, empty exporter",
			config: OTelTracerConfig{
				Disable: false,
			},
			expect: OTelTracerConfig{
				Disable: false,
			},
			expectErr: false,
		},
		{
			name: "enable, non-nil exporter",
			config: OTelTracerConfig{
				Disable: false,
				Exporter: OtelTracerExporter{
					GRPC: &OtelTracerGRPCExporter{
						Endpoint: "abc",
						Insecure: true,
					},
				},
			},
			expect: OTelTracerConfig{
				Disable: false,
				Exporter: OtelTracerExporter{
					GRPC: &OtelTracerGRPCExporter{
						Endpoint: "abc",
						Insecure: true,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "enable, nil exporter with env-var grpc",
			setenvFunc: func(t *testing.T) {
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER", "grpc")
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT", "abc")
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT_INSECURE", "true")
			},
			config: OTelTracerConfig{
				Disable: false,
			},
			expect: OTelTracerConfig{
				Disable: false,
				Exporter: OtelTracerExporter{
					GRPC: &OtelTracerGRPCExporter{
						Endpoint: "abc",
						Insecure: true,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "enable, nil exporter with env-var http",
			setenvFunc: func(t *testing.T) {
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER", "grpc")
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT", "abc")
				t.Setenv("SRUN_OTEL_TRACE_EXPORTER_ENDPOINT_INSECURE", "true")
			},
			config: OTelTracerConfig{
				Disable: false,
			},
			expect: OTelTracerConfig{
				Disable: false,
				Exporter: OtelTracerExporter{
					HTTP: &OtelTracerHTTPExporter{
						Endpoint: "abc",
						Insecure: true,
					},
				},
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.setenvFunc != nil {
				test.setenvFunc(t)
			}
			err := test.config.Validate()
			if (err != nil) != test.expectErr {
				t.Fatalf("expecting error %v but got %v", test.expectErr, err)
			}
		})
	}
}

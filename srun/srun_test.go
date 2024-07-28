package srun

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var _ ServiceRunnerAware = (*serviceDoNothing)(nil)

// TestRunReturn runs sequentially because we use t.Setenv which cannot be run in parallel.
func TestRunReturn(t *testing.T) {
	errReturn := errors.New("some error")

	tests := []struct {
		name   string
		run    func(ctx context.Context, r ServiceRunner) error
		env    map[string]string
		expect func(*testing.T, error)
	}{
		{
			name: "return nil",
			run: func(ctx context.Context, r ServiceRunner) error {
				return nil
			},
			expect: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("expecting nil but got %v", err)
				}
			},
		},
		{
			name: "return error",
			run: func(ctx context.Context, r ServiceRunner) error {
				return errReturn
			},
			expect: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, errReturn) {
					t.Fatalf("expecting %v but got %v", errReturn, err)
				}
			},
		},
		{
			name: "panic",
			run: func(ctx context.Context, r ServiceRunner) error {
				panic("a panic")
			},
			expect: func(t *testing.T, err error) {
				t.Helper()

				if !errors.Is(err, errPanic) {
					t.Fatalf("expecting %v but got %v", errPanic, err)
				}
			},
		},
		{
			name: "run deadline",
			run: func(ctx context.Context, r ServiceRunner) error {
				return Serve("run-readline-service", r, func(ctx Context) error {
					time.Sleep(time.Second * 5)
					return nil
				})
			},
			expect: func(t *testing.T, err error) {
				t.Helper()

				if !errors.Is(err, errRunDeadlineTimeout) {
					t.Fatalf("expecting error %v but got %v", errRunDeadlineTimeout, err)
				}
			},
			env: map[string]string{
				"SRUN_DEADLINE_TIMEOUT": "3s",
			},
		},
		{
			name: "graceful timeout",
			run: func(ctx context.Context, r ServiceRunner) error {
				return Serve("run-graceful-timeout", r, func(ctx Context) error {
					time.Sleep(time.Second * 5)
					return nil
				})
			},
			expect: func(t *testing.T, err error) {
				t.Helper()

				if !errors.Is(err, errRunDeadlineTimeout) {
					t.Fatalf("expecting error %v but got %v", errRunDeadlineTimeout, err)
				}
			},
			env: map[string]string{
				"SRUN_DEADLINE_TIMEOUT": "3s",
			},
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(test.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			conf := Config{
				ServiceName: "testing",
				Admin: AdminConfig{
					Disable: true,
				},
				OtelTracer: OTelTracerConfig{
					Disable: true,
				},
				OtelMetric: OtelMetricConfig{
					Disable: true,
				},
			}
			err := New(conf).Run(tt.run)
			tt.expect(t, err)
		})
	}
}

// TestGracefulShutdown test whether the graceful shutdown period is respected and the program exit when deadline is reached.
func TestGracefulShutdown(t *testing.T) {
	t.Run("graceful_shutdown", func(t *testing.T) {
		t.Parallel()
		config := Config{
			ServiceName: "test_graceful_shutdown",
			Admin:       AdminConfig{Disable: true},
			OtelTracer:  OTelTracerConfig{Disable: true},
			OtelMetric:  OtelMetricConfig{Disable: true},
		}
		err := New(config).Run(func(ctx context.Context, runner ServiceRunner) error {
			return Serve("testing", runner, func(ctx Context) error {
				time.Sleep(time.Second)
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("graceful_shutdown_timeout", func(t *testing.T) {
		t.Parallel()
		config := Config{
			ServiceName: "testing_graceful_shutdown_timeout",
			Admin:       AdminConfig{Disable: true},
			OtelTracer:  OTelTracerConfig{Disable: true},
			OtelMetric:  OtelMetricConfig{Disable: true},
			Timeout: TimeoutConfig{
				ShutdownGracefulPeriod: time.Second,
			},
		}
		err := New(config).Run(func(ctx context.Context, runner ServiceRunner) error {
			lrt1, err := NewLongRunningTask("exit early", func(ctx Context) error {
				return errors.New("exit now")
			})
			if err != nil {
				return err
			}
			lrt2, err := NewLongRunningTask("exit later", func(ctx Context) error {
				time.Sleep(time.Second * 5)
				return nil
			})
			if err != nil {
				return err
			}
			return runner.Register(lrt2, lrt1)
		})
		if !errors.Is(err, errGracefulPeriodTimeout) {
			t.Fatalf("expecting error %v but got %v", errGracefulPeriodTimeout, err)
		}
	})

	t.Run("graceful_shutdown_no_timeout", func(t *testing.T) {
		t.Parallel()
		config := Config{
			ServiceName: "testing_graceful_shutdown_timeout",
			Admin:       AdminConfig{Disable: true},
			OtelTracer:  OTelTracerConfig{Disable: true},
			OtelMetric:  OtelMetricConfig{Disable: true},
			Timeout: TimeoutConfig{
				ShutdownGracefulPeriod: time.Second,
			},
		}
		err := New(config).Run(func(ctx context.Context, runner ServiceRunner) error {
			return Serve("testing", runner, func(ctx Context) error {
				time.Sleep(time.Second * 5)
				return nil
			})
		})
		if errors.Is(err, errGracefulPeriodTimeout) {
			t.Fatalf("expecting error nil but got %v", err)
		}
	})
}

// TestServiceStateLog tests the order of the service state by looking at the log output.
// In this test we also ensure we are starting and stopping the services in the correct order.
func TestServiceStateLog(t *testing.T) {
	expect := `level=INFO msg="Running program: testing"
level=INFO msg="[Service] testing_1: INITIATING..."
level=INFO msg="[Service] testing_1: INITIATED"
level=INFO msg="[Service] testing_1: STARTING..."
level=INFO msg="[Service] testing_1: RUNNING"
level=INFO msg="[Service] testing_2: INITIATING..."
level=INFO msg="[Service] testing_2: INITIATED"
level=INFO msg="[Service] testing_2: STARTING..."
level=INFO msg="[Service] testing_2: RUNNING"
level=INFO msg="[Service] testing_3: INITIATING..."
level=INFO msg="[Service] testing_3: INITIATED"
level=INFO msg="[Service] testing_3: STARTING..."
level=INFO msg="[Service] testing_3: RUNNING"
level=INFO msg="[Service] testing_3: SHUTTING DOWN..."
level=INFO msg="[Service] testing_3: STOPPED"
level=INFO msg="[Service] testing_2: SHUTTING DOWN..."
level=INFO msg="[Service] testing_2: STOPPED"
level=INFO msg="[Service] testing_1: SHUTTING DOWN..."
level=INFO msg="[Service] testing_1: STOPPED"
`

	buff := bytes.NewBuffer(nil)
	config := Config{
		ServiceName: "testing",
		Admin:       AdminConfig{Disable: true},
		OtelTracer:  OTelTracerConfig{Disable: true},
		OtelMetric:  OtelMetricConfig{Disable: true},
		Logger: LoggerConfig{
			Format: LogFormatText,
			Output: buff,
			// Log is ordered, so we can ignore the time.
			RemoveTime: true,
		},
	}
	err := New(config).Run(func(ctx context.Context, runner ServiceRunner) error {
		if err := Serve("testing_1", runner, func(ctx Context) error {
			time.Sleep(time.Second)
			return nil
		}); err != nil {
			return err
		}
		if err := Serve("testing_2", runner, func(ctx Context) error {
			time.Sleep(time.Second)
			return nil
		}); err != nil {
			return err
		}
		if err := Serve("testing_3", runner, func(ctx Context) error {
			time.Sleep(time.Second)
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	got := buff.String()
	if diff := cmp.Diff(expect, got); diff != "" {
		t.Logf("expect\n%s", expect)
		t.Logf("got\n%s", got)
		t.Fatalf("(-want/+got)\n%s", diff)
	}
}

func TestServiceStateTracker(t *testing.T) {
	t.Parallel()
	tracker := newServiceStateTracker(&serviceDoNothing{}, slog.Default())

	tests := []struct {
		name string
		fn   func(context.Context) error
		err  error
	}{
		{
			name: "init",
			fn:   func(ctx context.Context) error { return tracker.Init(Context{}) },
			err:  nil,
		},
		{
			name: "try to run",
			fn:   tracker.Run,
			err:  nil,
		},
		{
			name: "try to run again, should got error already running",
			fn:   tracker.Run,
			err:  errServiceRunnerAlreadyRunning,
		},
		{
			name: "invoke ready",
			fn:   tracker.Ready,
			err:  nil,
		},
		{
			name: "invoke ready again, should return nil",
			fn:   tracker.Ready,
			err:  nil,
		},
		{
			name: "stop the tracker",
			fn:   tracker.Stop,
			err:  nil,
		},
		{
			name: "stop the tracker again, should return nil",
			fn:   tracker.Stop,
			err:  nil,
		},
		{
			name: "try to init again",
			fn:   func(ctx context.Context) error { return tracker.Init(Context{}) },
			err:  nil,
		},
		{
			name: "try to run again",
			fn:   tracker.Run,
			err:  nil,
		},
		{
			name: "invoke ready again, after stopped",
			fn:   tracker.Ready,
			err:  nil,
		},
		{
			name: "try to stop again",
			fn:   tracker.Stop,
			err:  nil,
		},
	}

	for _, test := range tests {
		t.Log(test.name)
		if err := test.fn(context.Background()); err != test.err {
			t.Fatalf("expecting error %v but got %v", test.err, err)
		}
	}
}

func TestServiceStartOrder(t *testing.T) {
	t.Run("normal flow", func(t *testing.T) {
		tracker := newServiceStateTracker(&serviceDoNothing{}, slog.Default())
		// Run before init.
		if err := tracker.Run(context.Background()); !errors.Is(err, errInvalidStateOrder) {
			t.Fatalf("run: expecting error %v but got %v", errInvalidStateOrder, err)
		}
		// Init.
		if err := tracker.Init(Context{}); err != nil {
			t.Fatal(err)
		}
		// Check Ready before Run.
		if err := tracker.Ready(context.Background()); !errors.Is(err, errInvalidStateOrder) {
			t.Fatalf("ready: expecting error %v but got %v", errInvalidStateOrder, err)
		}
		// Run.
		if err := tracker.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Ready.
		if err := tracker.Ready(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Stop.
		if err := tracker.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ready racy flow", func(t *testing.T) {
		tracker := newServiceStateTracker(&serviceDoNothing{}, slog.Default())
		// Init.
		if err := tracker.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		errC := make(chan error, 1)
		go func() {
			errC <- tracker.Ready(context.Background())
		}()
		go func() {
			time.Sleep(time.Millisecond * 200)
			errC <- tracker.Run(context.Background())
		}()

		for range 2 {
			err := <-errC
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

type serviceDoNothing struct {
	name   string
	logger *slog.Logger
	onRun  func(ctx context.Context, sdn *serviceDoNothing) error
}

func (s *serviceDoNothing) Name() string {
	if s.name == "" {
		return "service_do_nothing"
	}
	return s.name
}

func (s *serviceDoNothing) Init(ctx Context) error {
	s.logger = ctx.Logger
	return nil
}

func (s *serviceDoNothing) Run(ctx context.Context) error {
	if s.onRun != nil {
		if err := s.onRun(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (s *serviceDoNothing) Ready(ctx context.Context) error {
	return nil
}

func (s *serviceDoNothing) Stop(ctx context.Context) error {
	return nil
}

func TestServiceLogScope(t *testing.T) {
	t.Run("srun logger", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		expectLog := `level=INFO msg="this is a log" logger_scope=service_runner
`
		s := New(Config{
			ServiceName: "log_group",
			Logger: LoggerConfig{
				Output:     buff,
				RemoveTime: true,
			},
		})
		s.logger.Info("this is a log")

		got := buff.String()
		if diff := cmp.Diff(expectLog, got); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})

	t.Run("registrar logger", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		expectLog := `level=INFO msg="this is a log" logger_scope=log_group
`

		r := newRegistrar(New(Config{
			ServiceName: "log_group",
			Logger: LoggerConfig{
				Output:     buff,
				RemoveTime: true,
			},
		}))
		r.context.Logger.Info("this is a log")

		got := buff.String()
		if diff := cmp.Diff(expectLog, got); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})

	t.Run("service logger", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		expectLog := `level=INFO msg="Running program: log_group" logger_scope=service_runner
level=INFO msg="[Service] a_service: INITIATING..." logger_scope=service_runner
level=INFO msg="[Service] a_service: INITIATED" logger_scope=service_runner
level=INFO msg="[Service] a_service: RUNNING" logger_scope=service_runner
level=INFO msg="[Service] a_service: STARTING..." logger_scope=service_runner
level=INFO msg="this is a log" logger_scope=a_service
level=INFO msg="[Service] a_service: SHUTTING DOWN..." logger_scope=service_runner
level=INFO msg="[Service] a_service: STOPPED" logger_scope=service_runner
`

		s := New(Config{
			ServiceName: "log_group",
			Admin: AdminConfig{
				Disable: true,
			},
			OtelTracer: OTelTracerConfig{
				Disable: true,
			},
			OtelMetric: OtelMetricConfig{
				Disable: true,
			},
			Logger: LoggerConfig{
				Output:     buff,
				RemoveTime: true,
			},
		})
		s.MustRun(func(ctx context.Context, runner ServiceRunner) error {
			sdn := &serviceDoNothing{
				name: "a_service",
				onRun: func(ctx context.Context, sdn *serviceDoNothing) error {
					sdn.logger.Info("this is a log")
					return nil
				},
			}
			return runner.Register(sdn)
		})

		got := buff.String()
		if diff := cmp.Diff(expectLog, got); diff != "" {
			t.Fatalf("(-want/+got)\n%s", diff)
		}
	})
}

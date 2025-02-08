package srun

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConcurrentServices(t *testing.T) {
	lrtFn1 := func(ctx Context) error {
		time.Sleep(time.Second * 3)
		return nil
	}
	lrtFn2 := func(ctx Context) error {
		time.Sleep(time.Second * 3)
		return nil
	}
	lrtFn3 := func(ctx Context) error {
		time.Sleep(time.Second * 3)
		return nil
	}
	// lrtFn4 only waits for 1 second and returns an error.
	lrtFn4 := func(ctx Context) error {
		time.Sleep(time.Second * 1)
		return errors.New("some error")
	}

	runnerConfig := Config{
		Name:       "concurrent-test",
		Admin:      AdminConfig{Disable: true},
		OtelTracer: OTelTracerConfig{Disable: true},
		OtelMetric: OtelMetricConfig{Disable: true},
		Logger: LoggerConfig{
			Format: LogFormatText,
			// Log is ordered, so we can ignore the time.
			RemoveTime: true,
		},
	}

	t.Run("name", func(t *testing.T) {
		r := New(runnerConfig)
		lrt1 := newLRT(t, "testing_1", lrtFn1)
		lrt2 := newLRT(t, "testing_2", lrtFn2)
		lrt3 := newLRT(t, "testing_3", lrtFn3)
		svc, err := BuildConcurrentServices(
			newRegistrar(r),
			lrt1,
			lrt2,
			lrt3,
		)
		if err != nil {
			t.Fatal(err)
		}

		expect := "testing_1 | testing_2 | testing_3"
		if svc.Name() != expect {
			t.Fatalf("expecting %s but got %s", expect, svc.Name())
		}
	})
	t.Run("start", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		conf := runnerConfig
		conf.Logger.Output = buff

		r := New(conf)
		lrt1 := newLRT(t, "testing_1", lrtFn1)
		lrt2 := newLRT(t, "testing_2", lrtFn2)
		lrt3 := newLRT(t, "testing_3", lrtFn3)
		svc, err := BuildConcurrentServices(
			newRegistrar(r),
			lrt1,
			lrt2,
			lrt3,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := svc.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		errC := make(chan error)
		go func() {
			errC <- svc.Run(context.Background())
		}()
		// Wait until the service is ready.
		err = svc.Ready(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = <-errC
		if err != nil {
			t.Fatal(err)
		}
		// Check whether some logs are exists. The test is not deterministic because Go will schedule the goroutines based
		// on the availbility of the resource and runtime processes.
		checkExists := []string{
			`level=INFO msg="[Service] testing_1: INITIATING"`,
			`level=INFO msg="[Service] testing_1: INITIATED"`,
			`level=INFO msg="[Service] testing_1: STARTING"`,
			`level=INFO msg="[Service] testing_1: RUNNING"`,
			`level=INFO msg="[Service] testing_1: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_1: STOPPED"`,
			`level=INFO msg="[Service] testing_2: STARTING"`,
			`level=INFO msg="[Service] testing_2: RUNNING"`,
			`level=INFO msg="[Service] testing_2: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_2: STOPPED"`,
			`level=INFO msg="[Service] testing_3: STARTING"`,
			`level=INFO msg="[Service] testing_3: RUNNING"`,
			`level=INFO msg="[Service] testing_3: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_3: STOPPED"`,
		}
		logOutput := buff.String()

		for _, check := range checkExists {
			if !strings.Contains(logOutput, check) {
				t.Logf("output\n%s", logOutput)
				t.Fatalf("%s not exist in the log", check)
			}
		}
	})

	t.Run("start_stop", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		conf := runnerConfig
		conf.Logger.Output = buff

		r := New(conf)
		lrt1 := newLRT(t, "testing_1", lrtFn1)
		lrt2 := newLRT(t, "testing_2", lrtFn2)
		lrt3 := newLRT(t, "testing_3", lrtFn3)
		svc, err := BuildConcurrentServices(
			newRegistrar(r),
			lrt1,
			lrt2,
			lrt3,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := svc.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		errC := make(chan error)
		go func() {
			errC <- svc.Run(context.Background())
		}()
		// Wait until the service is ready.
		err = svc.Ready(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Stop the service before the service exiting.
		err = svc.Stop(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = <-errC
		if err != nil {
			t.Fatal(err)
		}

		// Check whether some logs are exists. The test is not deterministic because Go will schedule the goroutines based
		// on the availbility of the resource and runtime processes.
		checkExists := []string{
			`level=INFO msg="[Service] testing_1: INITIATING"`,
			`level=INFO msg="[Service] testing_1: INITIATED"`,
			`level=INFO msg="[Service] testing_1: STARTING"`,
			`level=INFO msg="[Service] testing_1: RUNNING"`,
			`level=INFO msg="[Service] testing_1: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_1: STOPPED"`,
			`level=INFO msg="[Service] testing_2: STARTING"`,
			`level=INFO msg="[Service] testing_2: RUNNING"`,
			`level=INFO msg="[Service] testing_2: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_2: STOPPED"`,
			`level=INFO msg="[Service] testing_3: STARTING"`,
			`level=INFO msg="[Service] testing_3: RUNNING"`,
			`level=INFO msg="[Service] testing_3: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_3: STOPPED"`,
		}
		logOutput := buff.String()

		for _, check := range checkExists {
			if !strings.Contains(logOutput, check) {
				t.Logf("output\n%s", logOutput)
				t.Fatalf("%s not exist in the log", check)
			}
		}
	})

	t.Run("start_stop_start", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		conf := runnerConfig
		conf.Logger.Output = buff

		r := New(conf)
		lrt1 := newLRT(t, "testing_1", lrtFn1)
		lrt2 := newLRT(t, "testing_2", lrtFn2)
		lrt3 := newLRT(t, "testing_3", lrtFn3)
		svc, err := BuildConcurrentServices(
			newRegistrar(r),
			lrt1,
			lrt2,
			lrt3,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := svc.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		errC := make(chan error)
		go func() {
			errC <- svc.Run(context.Background())
		}()
		// Wait until the service is ready.
		err = svc.Ready(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Stop the service before the service exiting.
		err = svc.Stop(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = <-errC
		if err != nil {
			t.Fatal(err)
		}

		// Check whether some logs are exists. The test is not deterministic because Go will schedule the goroutines based
		// on the availbility of the resource and runtime processes.
		checkExists := []string{
			`level=INFO msg="[Service] testing_1: INITIATING"`,
			`level=INFO msg="[Service] testing_1: INITIATED"`,
			`level=INFO msg="[Service] testing_1: STARTING"`,
			`level=INFO msg="[Service] testing_1: RUNNING"`,
			`level=INFO msg="[Service] testing_1: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_1: STOPPED"`,
			`level=INFO msg="[Service] testing_2: STARTING"`,
			`level=INFO msg="[Service] testing_2: RUNNING"`,
			`level=INFO msg="[Service] testing_2: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_2: STOPPED"`,
			`level=INFO msg="[Service] testing_3: STARTING"`,
			`level=INFO msg="[Service] testing_3: RUNNING"`,
			`level=INFO msg="[Service] testing_3: SHUTTING DOWN"`,
			`level=INFO msg="[Service] testing_3: STOPPED"`,
		}
		logOutput := buff.String()

		for _, check := range checkExists {
			if !strings.Contains(logOutput, check) {
				t.Logf("output\n%s", logOutput)
				t.Fatalf("%s not exist in the log", check)
			}
		}

		// Reset the buffer.
		buff.Reset()

		if err := svc.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		// Start the service again, but this time without calling the stop function.
		go func() {
			errC <- svc.Run(context.Background())
		}()
		// Wait until the service is ready.
		err = svc.Ready(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = <-errC
		if err != nil {
			t.Fatal(err)
		}

		// Re-check all the logs and ensure we have the same state.
		t.Log("Re-check log")
		for _, check := range checkExists {
			if !strings.Contains(logOutput, check) {
				t.Logf("output\n%s", logOutput)
				t.Fatalf("%s not exist in the log", check)
			}
		}
	})

	t.Run("start_error", func(t *testing.T) {
		buff := bytes.NewBuffer(nil)
		conf := runnerConfig
		conf.Logger.Output = buff

		r := New(conf)
		lrt1 := newLRT(t, "testing_1", lrtFn1)
		lrt2 := newLRT(t, "testing_2", lrtFn2)
		lrt3 := newLRT(t, "testing_3", lrtFn3)
		lrt4 := newLRT(t, "testing_4", lrtFn4)
		svc, err := BuildConcurrentServices(
			newRegistrar(r),
			lrt1,
			lrt2,
			lrt3,
			lrt4,
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := svc.Init(Context{}); err != nil {
			t.Fatal(err)
		}

		errC := make(chan error)
		go func() {
			errC <- svc.Run(context.Background())
		}()
		// Wait until the service is ready. The test is not failing in the ready state because the lrtFn4 is failing after 1 second
		// and for LongRunningTask the wait ready is only 300 miliseconds. So we should expect the error to appear in the error channel.
		err = svc.Ready(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Stop the service before the service exiting.
		err = svc.Stop(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = <-errC
		if err == nil {
			t.Fatal("expecting error but got nil")
		}
	})
}

func TestLongRunningTask(t *testing.T) {
	tests := []struct {
		name    string
		task    func(ctx Context) error
		runCtx  context.Context
		runErr  error
		stopCtx func() (context.Context, context.CancelFunc)
		stopErr error
	}{
		{
			name: "run, wait for exit",
			task: func(ctx Context) error {
				time.Sleep(time.Second)
				return nil
			},
			runCtx: context.Background(),
			runErr: nil,
		},
		{
			name: "run, wait for graceful",
			task: func(ctx Context) error {
				time.Sleep(time.Second)
				return nil
			},
			runCtx: context.Background(),
			runErr: nil,
			stopCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Second*2)
			},
			stopErr: nil,
		},
		{
			name: "run, stop deadline exceeded",
			task: func(ctx Context) error {
				time.Sleep(time.Second)
				return nil
			},
			runCtx: context.Background(),
			runErr: errLongRunningTaskStopDeadline,
			stopCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Millisecond)
			},
			stopErr: nil,
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(test.name, func(t *testing.T) {
			lrt, err := NewLongRunningTask(tt.name, tt.task)
			if err != nil {
				t.Fatal(err)
			}

			errC := make(chan error, 1)
			go func() {
				errC <- lrt.Run(tt.runCtx)
			}()

			if tt.stopCtx != nil {
				ctx, cancel := tt.stopCtx()
				defer cancel()

				err := lrt.Stop(ctx)
				if !errors.Is(err, tt.stopErr) {
					t.Fatalf("expecting stop error %v but got %v", tt.stopErr, err)
				}
			}

			err = <-errC
			if !errors.Is(err, test.runErr) {
				t.Fatalf("expecting run error %v but got %v", tt.runErr, err)
			}
		})
	}
}

func TestStateHelper(t *testing.T) {
	t.Parallel()

	name := "testing"
	s := &StateHelper{
		serivceName: name,
	}
	if s.ServiceName() != name {
		t.Fatalf("expecting %s but got %s", name, s.ServiceName())
	}
	s.SetRunning()
	if !s.IsRunning() && s.get() != int(serviceStateRunning) {
		t.Fatalf("service is not in running state, got %d", s.get())
	}
	s.SetStopped()
	if !s.IsStopped() && s.get() != int(serviceStateStopped) {
		t.Fatalf("service is not in running state, got %d", s.get())
	}
}

func newLRT(t *testing.T, name string, fn func(Context) error) *LongRunningTask {
	t.Helper()

	lrt, err := NewLongRunningTask(name, fn)
	if err != nil {
		t.Fatal(err)
	}
	return lrt
}

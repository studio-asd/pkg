package srun

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// ConcurrentServices wraps services that implements ServiceRunnerAware and starts them concurrenly.
//
// Sometimes we need some services to be started in concurrent to save time and its okay for them to
// start at almost the same time.
//
// The idea of concurrent services is not to disturb the main runner to schedule and watch the concurrent
// starts and stops of the services. So we can offload the logic and complexity completely to this service.
type ConcurrentServices struct {
	services     []*ServiceStateTracker
	runnerLogger *slog.Logger

	stopMu sync.Mutex
	stopC  chan struct{}
}

// BuildConcurrentServices build a new concurrent services so all services inside the concurrent services can be started concurrently. By default, runner
// runs all the service in FIFO-order.
//
// You can use this kind of service if you have some services that need to be started concurrently and beneficial for you to shorten the startup time.
// In our case, we usually use this service to starts our pub/sub consumers concurrently after all non-trivial services are up.
func BuildConcurrentServices(runner ServiceRunner, services ...ServiceRunnerAware) (*ConcurrentServices, error) {
	registrar, ok := runner.(*Registrar)
	if !ok {
		return nil, errors.New("the service runner type must be *Registrar")
	}
	return newConcurrentServices(registrar.runner.logger, services...)
}

func newConcurrentServices(runnerLogger *slog.Logger, services ...ServiceRunnerAware) (*ConcurrentServices, error) {
	csvc := &ConcurrentServices{
		runnerLogger: runnerLogger,
		stopC:        make(chan struct{}),
	}
	err := csvc.Register(services...)
	return csvc, err
}

func (c *ConcurrentServices) Register(services ...ServiceRunnerAware) error {
	for _, svc := range services {
		if _, ok := svc.(*ServiceStateTracker); ok {
			return errors.New("cannot use service state tracker as the type of concurrent services")
		}
		s := newServiceStateTracker(svc, c.runnerLogger)
		c.services = append(c.services, s)
	}
	return nil
}

// Name returns the name of the services with '|' as delimiter.
func (c *ConcurrentServices) Name() string {
	names := make([]string, len(c.services))
	for idx, svc := range c.services {
		names[idx] = svc.Name()
	}
	return strings.Join(names, " | ")
}

func (c *ConcurrentServices) Init(ctx Context) error {
	errGroup := errgroup.Group{}
	for _, svc := range c.services {
		s := svc
		errGroup.Go(func() error {
			return s.Init(Context{
				Ctx:    ctx.Ctx,
				Logger: c.runnerLogger.WithGroup(s.Name()),
				Meter:  ctx.Meter,
				Tracer: ctx.Tracer,
			})
		})
	}
	return errGroup.Wait()
}

// Run runs the services concurrently and wait for all services to stop before returning.
func (c *ConcurrentServices) Run(ctx context.Context) (err error) {
	errC := make(chan error, 1)
	for _, s := range c.services {
		svc := s
		go func() {
			errC <- svc.Run(ctx)
		}()
	}

	defer func() {
		errStop := c.Stop(ctx)
		if errStop != nil {
			errors.Join(err, errStop)
		}
	}()

	errCount := 0
	for {
		err = <-errC
		errCount++
		// Wait until all services stopped.
		if err == nil && errCount < len(c.services) {
			continue
		}
		return err
	}
}

// Ready listens to ready check notification from all services before notify ready to the upstream runner.
func (c *ConcurrentServices) Ready(ctx context.Context) error {
	maxRetry := 3
	retryDelay := time.Millisecond * 300

	// Wait until all services are ready. We don't need to check whether the service is stopped again because we
	// wrap the service with ServiceStateTracker.
	for _, svc := range c.services {
		var err error
		// Retry the readiness check until max retry attempt as we will run all the services inside goroutines. The goroutines
		// might not even scheduled yet when this function is called, so we need to check them with retries.
		for i := 0; i < maxRetry; i++ {
			err = svc.Ready(ctx)
			if err != nil && errors.Is(err, errCantCheckReadiness) {
				time.Sleep(retryDelay)
				continue
			}
			if err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Stop stops all running services with errgroup and return the first error if exist.
func (c *ConcurrentServices) Stop(ctx context.Context) error {
	c.stopMu.Lock()
	defer c.stopMu.Unlock()

	g, _ := errgroup.WithContext(ctx)
	for _, s := range c.services {
		svc := s
		g.Go(func() error {
			return svc.Stop(ctx)
		})
	}
	return g.Wait()
}

// NewLongRunningTask creates long running task with a name.
func NewLongRunningTask(name string, fn func(ctx Context) error) (*LongRunningTask, error) {
	if name == "" {
		return nil, errors.New("name cannot be empty")
	}
	if fn == nil {
		return nil, errors.New("serve func cannot be empty")
	}

	return &LongRunningTask{
		name:        name,
		fn:          fn,
		readyC:      make(chan struct{}, 1),
		stopC:       make(chan struct{}, 1),
		delayrReady: time.Millisecond * 300,
	}, nil
}

// LongRunningTask is usually used for trivial task like serving http server without using the
// internal package that aware of runner package. This means we can start the standard library
// http server easily using this object.
//
// Implements ServiceRunnerAware interface.
type LongRunningTask struct {
	name string
	iCtx Context

	errMu sync.Mutex
	err   error
	// readyC is used to notify that long running task is ready and running.
	readyC chan struct{}
	fn     func(ctx Context) error
	// delayReady is used to delay the ready notification
	delayrReady time.Duration

	stopMu  sync.Mutex
	stopped bool
	// stopC is used to stop the long running task and trigger the cancelfn. We are not using the cancelfn because it will trigger
	// a context cancelled error that might not behave as intended.
	stopC   chan struct{}
	stopCtx context.Context
}

// Name returns the name of the long running task.
func (l *LongRunningTask) Name() string {
	return l.name
}

// Init does nothing in the long-running-task as it only wraps function.
func (l *LongRunningTask) Init(ctx Context) error {
	l.iCtx = ctx
	return nil
}

func (l *LongRunningTask) Run(ctx context.Context) error {
	// Create a new context for cancellation because we will trigger the cancel context in the stop function.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		l.stopMu.Lock()
		l.stopped = true
		l.stopMu.Unlock()
	}()

	errC := make(chan error, 1)
	go func() {
		errC <- l.fn(Context{
			Ctx:            cancelCtx,
			Logger:         l.iCtx.Logger,
			Meter:          l.iCtx.Meter,
			Tracer:         l.iCtx.Tracer,
			HealthNotifier: l.iCtx.HealthNotifier,
		})
	}()
	// As of now we don't have any way to tell whether the process is running or not other than waiting
	// for a certain duration of time and mark the task as ready.
	time.Sleep(l.delayrReady)
	l.stopMu.Lock()
	l.stopped = false
	l.stopMu.Unlock()
	l.readyC <- struct{}{}

	// Create a tight loop so we can use the same loop to listen for ready notification and other notification
	// such as stop and error.
	select {
	case <-l.stopC:
		cancel()
		// Wait for the task to exit because we don't want to return before the goroutine is really exit.
		// Or if the stop context deadline is reached, we will just exit without waiting for the goroutine
		// to exit.
		select {
		case <-l.stopCtx.Done():
			return errLongRunningTaskStopDeadline
		case err := <-errC:
			return err
		}
	case err := <-errC:
		l.errMu.Lock()
		l.err = err
		l.errMu.Unlock()
		return err
	}
}

// Ready returns wheter the task is ready or not. It will return immediately after we spawn goroutines
// and waits for delay. In the time of delay, we hope that the goroutine already scheduled.
func (l *LongRunningTask) Ready(ctx context.Context) error {
	l.errMu.Lock()
	if l.err != nil {
		l.errMu.Unlock()
		return l.err
	}
	l.errMu.Unlock()
	<-l.readyC
	return nil
}

// Stop cancels the long running task context created in the run function.
//
// The function receive a context and will use that context for the deadline to stop.
// If the deadline exceeds, then it will just exit without waiting for the running goroutine
// to be fully stopped. This is the intended behavior because the 'service' need to ensure
// they also listening to the context cacellation being passed by the Run function.
func (l *LongRunningTask) Stop(ctx context.Context) error {
	l.stopMu.Lock()
	if l.stopped {
		l.stopMu.Unlock()
		return nil
	}

	l.stopCtx = ctx
	l.stopC <- struct{}{}
	l.stopMu.Unlock()
	return nil
}

// Serve creates a new LongRunningTask that implements ServiceRunnerAware. This allows the user to use runner for trivial usage.
func Serve(name string, runner ServiceRunner, fn func(ctx Context) error) error {
	lrt, err := NewLongRunningTask(name, fn)
	if err != nil {
		return err
	}
	return runner.Register(lrt)
}

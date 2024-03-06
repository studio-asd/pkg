package srun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var (
	_ ServiceRunner      = (*Registrar)(nil)
	_ ServiceRunnerAware = (*LongRunningTask)(nil)
	_ ServiceRunnerAware = (*ServiceStateTracker)(nil)
)

const (
	gracefulShutdownDefaultTimeout = time.Minute * 5
	serviceReadyDefaultTimeout     = time.Minute
	serviceInitDefaultTimeout      = time.Minute
)

type serviceState int

// String returns the state in string.
func (s serviceState) String() string {
	return []string{
		"UNKNOWN_STATE",
		"INITIATING",
		"INITIATED",
		"STARTING",
		"RUNNING",
		"SHUTTING DOWN",
		"STOPPED",
	}[s]
}

// Service state tracks the state of the service and what's happening to each service.
const (
	serviceStateInitiating serviceState = iota + 1
	serviceStateInitiated
	serviceStateStarting
	serviceStateRunning
	serviceStateShutdown
	serviceStateStopped
)

var (
	errServiceRunnerAlreadyRunning = errors.New("service runner is already running")
	errGracefulPeriodTimeout       = errors.New("graceful-period timeout")
	// errRunDeadlineTimeout being thrown when 'SRUN_DEADLINE' is being set and the runner has run beyond the deadline duration.
	errRunDeadlineTimeout = errors.New("run deadline timeout reached")
	// errServiceInitTimeout is the timeout for initing the service.
	errServiceInitTimeout  = errors.New("service init timeout reached")
	errServiceReadyTimeout = errors.New("service ready timeout reached")
	// errUpgrade is runner internal error when upgrading program. We will use cloudflare/tableflip to trigger exit signal and self-upgrade the binary.
	errUpgrade = errors.New("upgrade signal triggered, upgrading program")
	// errServiceError mark there is an error with one of the service so everything need to be stopped.
	errServiceError = errors.New("encountered error in one of the service")
	// errPanic thrown when panic happens so we can granularly set the error cause.
	errPanic = errors.New("panic occured")
	// errCantCheckReadiness thrown when service is shutting down or stopped as it doesn't makes sense to check for its readiness.
	errCantCheckReadiness = errors.New("cannot check service readiness from stopped service")
	// errLongRunningTaskStopDeadline is a specific error for the long-running-task if the stop deadline is exceeded and the goroutine
	// is not exiting within the duration.
	errLongRunningTaskStopDeadline = errors.New("long_running_task: stop deadline exceeded")
	// errUnhealthyService is used when we are failed to check the service health for the first time.
	errUnhealthyService = errors.New("healthcheck: service is not healthy")
)

// Context holds runner context including all objects that belong to the runner. For example we can pass logger and otel meter
// object via this context.
type Context struct {
	Ctx    context.Context
	Logger *slog.Logger
	// Meter is open telemetry metric meter object to record metrics via open telemetry provider. The provider exports the metric
	// via prometheus exporter.
	//
	// You need to pass/inject the meter object to another function/struct to use this meter.
	Meter  metric.Meter
	Tracer trace.Tracer
	// HealthNotifier is the healthcheck notifier
	HealthNotifier *HealthcheckNotifier
}

// ServiceRunnerAware interface defines that an object is aware that it needs to comply with the service runner
// semantics so we can easily integrate their lifecycle to the service runner.
//
// PreRun, Run and Stop functions injected by runner Context so the service can use runner properties in their program.
// The context is being passed as a struct value(without pointer) because it's not intended to be passed to any chield object
// or function. You can pass the things that you need instead the whole context.
type ServiceRunnerAware interface {
	// Name returns the name of the service.
	Name() string
	// Init initialize the service by passing the srun.Context, so the service can use the special context object.
	Init(Context) error
	Run(context.Context) error
	// Ready returns the status for the service whether it is ready or not. The readiness state of a service
	// will block another service from running as we want to run them sequentially.
	//
	// The function use read only channel as we only need to listen from the channel and block until the service
	// is in ready state.
	Ready(context.Context) error
	Stop(context.Context) error
}

// ServiceUpgraderAware defines service that aware with the existence of an upgrader in the service runner.
// The service then delegates the setup of net.Listener to the upgrader because the upgrader need to pass all
// file descriptors to the new process.
type ServiceUpgraderAware interface {
	RequiredListener() (network, addr string)
	RegisterListener(listener net.Listener)
}

// ServiceRunner interface is a special type of interface that implemented by its own package to minimize the
// API surface for the users. We don't want to expose Run() method, so we need to use an interface.
//
// Please NOTE that this is a rare case where we want to use the concrete type in this package to implement the
// interface for the reason above. Usually the implementor of the interface should belong to the other/implementation package.
type ServiceRunner interface {
	Register(services ...ServiceRunnerAware) error
	Context() Context
}

type Registrar struct {
	runner  *Runner
	context Context
}

func (r *Registrar) Register(services ...ServiceRunnerAware) error {
	return r.runner.register(services...)
}

func (r *Registrar) Context() Context {
	return r.context
}

func newRegistrar(r *Runner) *Registrar {
	return &Registrar{
		runner: r,
		context: Context{
			Logger: slog.Default().WithGroup(r.config.ServiceName),
			Meter:  r.otelMeter,
			Tracer: r.otelTracer,
		},
	}
}

// Runner implements ServiceRunner.
type Runner struct {
	config      *Config
	serviceName string
	// Services is the list of objects that can be controlled by the runner.
	services []*ServiceStateTracker

	// logger is the default slog.Logger with group for runner. We will use this logger
	// to log instead of the global slog.
	logger *slog.Logger
	// ctxSignal is a context to wait for the exit signals.
	ctxSignal       context.Context
	ctxSignalCancel func()

	// upgrader instance to allow the program to self-upgrade using cloudflare/tableflip.
	upgrader *upgrader

	// otelTracer is open telemetry tracer instance to collect trace spans in application.
	otelTracer trace.Tracer
	// otelMeter is open telemetry meter instance to collect metrics in application.
	otelMeter metric.Meter
	// healthcheckService provide healthchecks for all services and multiplex the check notification.
	healthcheckService *HealthcheckService
}

type Config struct {
	// ServiceName defines the service name and the pid file name.
	ServiceName string
	Upgrader    UpgraderConfig
	Admin       AdminConfig
	OtelTracer  OTelTracerConfig
	OtelMetric  OtelMetricConfig
	Logger      LoggerConfig
	Healthcheck HealthcheckConfig
	Timeout     TimeoutConfig
	// deadlineDuration is the timeout duration for the runner to run. The program will exit with
	// ErrRunDeadlineTimeout when deadline exceeded.
	//
	// To enable run deadline, please use 'SRUN_DEADLINE_TIMEOUT' environment variable. For example SRUN_DEADLINE_TIMEOUT=30s.
	//
	// This feature is useful for several reasons:
	//	1. We can use it to test our binary to check whether it really runs or not.
	//	2. We can use it to limit the execution time in an environment like function as a service.
	DeadlineDuration time.Duration
}

func (c *Config) Validate() error {
	if c.ServiceName == "" {
		return errors.New("service name cannot be empty")
	}
	if c.Timeout.InitTimeout == 0 {
		c.Timeout.InitTimeout = serviceInitDefaultTimeout
	}
	if c.Timeout.ReadyTimeout == 0 {
		c.Timeout.ReadyTimeout = serviceReadyDefaultTimeout
	}
	if c.Timeout.ShutdownGracefulPeriod == 0 {
		c.Timeout.ShutdownGracefulPeriod = gracefulShutdownDefaultTimeout
	}
	if c.Healthcheck.Interval == 0 {
		c.Healthcheck.Interval = healthcheckDefaultInterval
	}
	if c.Healthcheck.Timeout == 0 {
		c.Healthcheck.Timeout = healthcheckDefaultTimeout
	}

	// Respect the configuration from environment variable if available.
	envReadyTimeout := os.Getenv("SRUN_READY_TIMEOUT")
	if envReadyTimeout != "" {
		readyTimeout, err := time.ParseDuration(envReadyTimeout)
		if err != nil {
			return err
		}
		c.Timeout.ReadyTimeout = readyTimeout
	}
	envDeadlineTimeout := os.Getenv("SRUN_DEADLINE_TIMEOUT")
	if envDeadlineTimeout != "" {
		deadlineTimeout, err := time.ParseDuration(envDeadlineTimeout)
		if err != nil {
			return err
		}
		c.DeadlineDuration = deadlineTimeout
	}
	envGracefulTimeout := os.Getenv("SRUN_GRACEFUL_TIMEOUT")
	if envGracefulTimeout != "" {
		gracefulTimeout, err := time.ParseDuration(envGracefulTimeout)
		if err != nil {
			return err
		}
		c.Timeout.ShutdownGracefulPeriod = gracefulTimeout
	}
	return nil
}

// TimeoutConfig is timeout configuration for several configurable configurations.
type TimeoutConfig struct {
	// InitTimeout is the timeout to initiate a service. The timeout is per-service and not the total duration of initialization.
	InitTimeout time.Duration
	// ReadyTimeout is the timeout to wait for a service to be ready. The timeout is per-service and not the total duration of ready wait.
	ReadyTimeout time.Duration
	// ShutdownGracefulPeriod is the timeout for runner waiting for all services to stop.
	ShutdownGracefulPeriod time.Duration
}

// New creates a new service runner to control the lifecycle of the service.
//
// Please NOTE this function will panic if it encounter errors. This is expected by design as this function will
// always be called inside func main.
func New(config Config) *Runner {
	conf := &config
	if err := conf.Validate(); err != nil {
		panic(err)
	}
	setDefaultSlog(conf.Logger)

	var (
		upg *upgrader
		err error
		ctx = context.Background()
	)

	if config.Upgrader.SelfUpgrade {
		pidFile := fmt.Sprintf("%s.pid", config.ServiceName)
		upg, err = newUpgrader(pidFile, syscall.SIGHUP)
		if err != nil {
			panic(err)
		}
		// Use the upgrader context as the base context, so if the upgrade exits, the runner will
		// also exit.
		ctx = upg.Context()
	}

	// Listen to the signal to exit the program.
	ctxSignal, cancel := signal.NotifyContext(
		ctx,
		syscall.SIGKILL,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
	)

	meter, meterLrt, err := newOtelMetricMeterAndProviderService(config.OtelMetric)
	if err != nil {
		panic(err)
	}
	tracer, tracerLrt, err := newOTelTracerService(config.OtelTracer)
	if err != nil {
		panic(err)
	}

	r := &Runner{
		serviceName:     config.ServiceName,
		config:          conf,
		logger:          slog.Default().WithGroup("service-runner"),
		ctxSignal:       ctxSignal,
		ctxSignalCancel: cancel,
		upgrader:        upg,
		otelMeter:       meter,
		otelTracer:      tracer,
	}
	if err := r.registerDefaultServices(tracerLrt, meterLrt); err != nil {
		panic(err)
	}
	return r
}

// register all services that needs to be run and controlled by the service runner.
func (r *Runner) register(services ...ServiceRunnerAware) error {
	if len(services) == 0 {
		return errors.New("register called with no service provided")
	}

	// Init is controlled by the init timeout and errgroup. We will init all the services concurrently.
	initTimeoutCtx, cancel := context.WithTimeout(r.ctxSignal, r.config.Timeout.InitTimeout)
	defer cancel()
	group, _ := errgroup.WithContext(initTimeoutCtx)
	group.SetLimit(runtime.NumCPU())

	for _, svc := range services {
		// Register the service to the healthcheck service so it is aware of the number of services and consumers.
		if r.healthcheckService != nil {
			if err := r.healthcheckService.register(svc); err != nil {
				return err
			}
		}
		// Check if the upgrader is initiated and service is upgrade aware. We need to override the service so we could pass listener that
		// created by the upgrader.
		if r.upgrader != nil {
			upgradeAware, ok := svc.(ServiceUpgraderAware)
			if !ok {
				continue
			}
			network, addr := upgradeAware.RequiredListener()
			listener, err := r.upgrader.createListener(network, addr)
			if err != nil {
				return err
			}
			// Put the created listener from the upgrader to the service. This way, we can transfer the listener file descriptor
			// of the service when upgrade happen.
			upgradeAware.RegisterListener(listener)
		}
		// Wrap ALL services using ServiceState tracker as we need to track the status/state of all services.
		r.services = append(r.services, newServiceStateTracker(svc, r.logger))

		// Init the service and stop them in-case of error. This is because in the init state the service might doing something that need
		// some cleanup in the later(stop) state. This also ensure the service is always in a 'stopped' state.
		s := svc
		group.Go(func() error {
			err := s.Init(Context{})
			if err == nil {
				return nil
			}
			errStop := s.Stop(initTimeoutCtx)
			if errStop != nil {
				err = errors.Join(err, errStop)
			}
			return err
		})
	}
	return group.Wait()
}

// onceRegisterServices ensure the default service registration happening only once.
var onceRegisterServices sync.Once

func (r *Runner) registerDefaultServices(otelTracerProvider, otelMeterProvider *LongRunningTask) error {
	var err error
	onceRegisterServices.Do(func() {
		// If the length of the admin configuration is not disabled, then we should always register
		// the http admin server.
		//
		// This way, the admin http server will always at the bottom of the stack and will be shuted-down last. This means
		// many things:
		//	1. The prometheus metrics is available in shutting down mode.
		//	2. The profile export is available in shutting down mode.
		//	3. We can listen/watch to the service shutdown.
		if !r.config.Admin.Disable {
			var adminServer *adminHTTPServer
			adminServer, err = newAdminServer(r.config.Admin.AdminServerConfig)
			if err != nil {
				return
			}
			r.services = append(r.services, newServiceStateTracker(adminServer, r.logger))
		}
		// If the healthcheck is not disabled, then we should spawn a healthcheck service.
		if !r.config.Healthcheck.Disable {
			hcs := newHealthcheckService(r.config.Healthcheck)
			r.services = append(r.services, newServiceStateTracker(hcs, r.logger))
			r.healthcheckService = hcs
		}
		// If the opentelemetry is not disabled, then start the open telemetry process using the long running task.
		if otelTracerProvider != nil {
			r.services = append(r.services, newServiceStateTracker(otelTracerProvider, r.logger))
		}
		// If the metric provider is not nil then we should listen to the shutdown event and shutdown the provider properly.
		if otelMeterProvider != nil {
			r.services = append(r.services, newServiceStateTracker(otelMeterProvider, r.logger))
		}
		// Listen to the upgrader to upgrade the binary using SIGHUP.
		if r.upgrader != nil {
			r.services = append(r.services, newServiceStateTracker(r.upgrader, r.logger))
		}
	})
	return err
}

// Run runs the run function that register services in the main function.
//
// Please NOTE that the run function should not block, otherwise  the runner can't execute other services that registered in the runner.
func (r *Runner) Run(run func(ctx context.Context, runner ServiceRunner) error) (err error) {
	r.logger.Info(fmt.Sprintf("Running program: %s", r.serviceName))

	var (
		readyTimeout            = r.config.Timeout.ReadyTimeout
		gracefulShutdownTimeout = r.config.Timeout.ShutdownGracefulPeriod
	)

	// Set the state of the service runner to run/not running and catch panic to enrich the error.
	defer func() {
		// Ensure no context is leaking and not cancelled.
		r.ctxSignalCancel()

		v := recover()
		if v != nil {
			errRecover, ok := v.(error)
			if ok {
				err = errors.Join(err, errRecover)
				err = errors.Join(err, errPanic)
			} else {
				err = errors.Join(err, fmt.Errorf("%v", v))
				err = errors.Join(err, errPanic)
			}
		}
		// Check if th error is expected or not upon exit. Because we send the cancelled context error
		// with cause to determine why the program exit.
		if !isError(err) {
			return
		}

		stackTrace := debug.Stack()
		// Wrap the error with additional information of stack trace.
		err = fmt.Errorf("%w\n\n%s", err, string(stackTrace))
	}()

	// If the deadline duration of the runner is not zero, then we should respect the deadline. By using deadline, it means the program
	// will exit if the deadline is reached.
	//
	// The deadline is being set here to be as close as possible to the run function.
	if r.config.DeadlineDuration > 0 {
		r.ctxSignal, r.ctxSignalCancel = context.WithDeadlineCause(r.ctxSignal, time.Now().Add(r.config.DeadlineDuration), errRunDeadlineTimeout)
	}

	err = run(r.ctxSignal, newRegistrar(r))
	if err != nil {
		return
	}
	// If we don't have any services, then don't bother to run anything at all.
	if len(r.services) == 0 {
		return nil
	}

	// Don't forget to stop all the services to ensure we are not leaking any resources behind.
	// We put the defer on-top for of triggering the run becauase we want to ensure if something
	// bad happen in the run function, we will still stop all the services.
	//
	// We need to stop all the services in LIFO order to ensure we prevent incoming traffic in
	// case of a service registering a http/gRPC server at the bottom of the service stack.
	// This actually match with what defer did. Defered functions will be executed from bottom.
	//
	// Imagine the services stack looked like this, the same with the one started above:
	//	|-------------------|
	//	|resource-controller|
	//	|    http-server   	|
	//	|    grpc-server	| <- exit_first
	//	|-------------------|
	//
	// Then the grpc-server will stopped first, then http-server. And after all traffic is stopped then
	// the resource-controller will be stopped after ensuring all connections are dropped/finished.
	//
	// We stopped the services inside a goroutine to ensure there are no blockers in the shutdown process
	// and we will wait until the graceful period timeout.
	defer func() {
		doneCh := make(chan struct{})
		ctxTimeout, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		// Invoke a goroutine and stop the services sequentially because we don't want to kill the services randomly.
		go func() {
			for i := len(r.services); i > 0; i-- {
				errStop := r.services[i-1].Stop(ctxTimeout)
				if errStop != nil {
					err = errors.Join(err, errStop)
				}
			}
			doneCh <- struct{}{}
		}()

		select {
		case <-ctxTimeout.Done():
			err = errors.Join(err, errGracefulPeriodTimeout)
			break
		case <-doneCh:
			break
		}
		cancel()
	}()

	errC := make(chan error, 1)
	// Start the service with FIFO, as we want to ensure the service at the bottom of the stack will be always
	// ready to start. For example, this behavior is beneficial when we start http/gRPC server after we are
	// connected to all dependencies.
	//
	// Imagine the services stack looked like this:
	//	|-------------------|
	//	|resource-controller| <- run_first
	//	|    http-server   	|
	//	|    grpc-server	|
	//	|-------------------|
	//
	// The resource controller will connects all databases and service dependencies first, then start the http-server
	// and then grpc-server last.
	for _, service := range r.services {
		svc := service
		// Init the service.
		initCtx, cancel := context.WithTimeout(r.ctxSignal, r.config.Timeout.InitTimeout)
		go func() {
			initContext := Context{
				Ctx:            initCtx,
				Logger:         r.logger.WithGroup(svc.Name()),
				Meter:          r.otelMeter,
				Tracer:         r.otelTracer,
				HealthNotifier: &HealthcheckNotifier{noop: true},
			}
			if r.healthcheckService != nil {
				initContext.HealthNotifier = r.healthcheckService.notifiers[svc]
			}
			err := svc.Init(initContext)
			errC <- err
		}()
		select {
		case <-initCtx.Done():
			cancel()
			return errServiceInitTimeout
		case err := <-errC:
			cancel()
			if err != nil {
				return err
			}
		}
		// Run the service.
		go func(s *ServiceStateTracker) {
			err := s.Run(r.ctxSignal)
			errC <- err
		}(svc)
		// Check whether the service is in ready state or not. We use backoff, because sometimes the goroutines is not scheduled
		// yet, thus lead to wrong result.
		readyCheckBackoff := time.Millisecond * 300
		readyC := make(chan error, 1)
		// Create a timeout for service readiness as we don't want to wait for too long for unresponsive service.
		readyTimeoutCtx, cancelReady := context.WithTimeout(r.ctxSignal, readyTimeout)
		defer cancelReady()
		// Spawn a goroutine to wait for the ready notification.
		go func() {
			// The loop and delay here is just to ensure we are waiting for the service that just started. It will possibly
			// throw an error because the goroutine that triggers Run is not scheduled yet.
			for i := 0; i < 3; i++ {
				err := svc.Ready(readyTimeoutCtx)
				if err != nil && errors.Is(err, errCantCheckReadiness) {
					time.Sleep(readyCheckBackoff)
					continue
				}
				readyC <- nil
				return
			}
		}()

		// Wait for the service to be ready before starting the next service.
		select {
		// We don't have to wait until all services are started before listening to an error.
		case errCallback := <-errC:
			if errCallback == nil {
				continue
			}
			err = errCallback
			return
		case <-readyTimeoutCtx.Done():
			cancelReady()
			return errServiceReadyTimeout
		case err := <-readyC:
			if err != nil {
				return err
			}
		}

		// Don't do any healthcheck if the healthcheck service is disabled.
		if r.healthcheckService == nil {
			continue
		}
		// Do a firstround of healthcheck after the service is ready as we want to understand the health status of each service.
		status, err := r.healthcheckService.check(context.Background(), svc)
		if err != nil {
			// TODO: return a healthcheck error
			return err
		}
		if status <= HealthStatusUhealthy {
			return fmt.Errorf("%w with name %s. Status: %s", errUnhealthyService, svc.Name(), status)
		}
	}

	var errCounter int
	// There are several ways that srun can exit:
	//
	// 1. Interupt/termination of the program.
	//    In this case, we will shutdown everything in the defer loop.
	//
	// 2. One of the 'service' is exiting with non nil error.
	//    In this case, we will shutdown everything in the defer loop.
	//
	// 3. All services exiting with nil error.
	//    In this case, the code will touch defer loop, but everything already stopped.
	//    So we will not wait for all services to shutdown again.
	for {
		select {
		case <-r.ctxSignal.Done():
			err = context.Cause(r.ctxSignal)
			return

		case err = <-errC:
			errCounter++
			if err != nil {
				// If an error is not because an upgrade, add more context that the program exit because an error from a service.
				if !errors.Is(err, errUpgrade) {
					err = fmt.Errorf("%w:%v", errServiceError, err)
				}
				return
			}
			// We will ignore services that returned nil error if the number of running services is still
			// more than the number of returned error. But if there are no services left, then we should
			// return immediately.
			//
			// This allows something like resource controller to exit first while waiting for other services
			// to be finished or cancelled by the interrupt signal.
			if errCounter < len(r.services) {
				continue
			}
			return
		}
	}
}

// MustRun exit the program using exit 1. The function does not using panic because we don't want to print the stack trace and only the error.
func (r *Runner) MustRun(run func(ctx context.Context, runner ServiceRunner) error) {
	err := r.Run(run)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

// buildContext receive a service runner aware and build a special runner context just for the service.
func (r *Runner) buildContext(svc ServiceRunnerAware) Context {
	logger := r.logger.WithGroup(svc.Name())
	return Context{
		Logger: logger,
		Tracer: r.otelTracer,
		Meter:  r.otelMeter,
	}
}

// isError returns true if the error is not expected by the runner. This function is needed and a bit unfortunate because
// runner itself need to return the error and use its value as an information.
//
// There are only two errors that expected by runner:
//   - ErrUpgrade, which indicates the runner need to exit to start a new process.
//   - ErrRunDeadlineTimeout, which indicates the deadline timeout have been reached.
func isError(err error) bool {
	okErrors := []error{
		errUpgrade,
		errRunDeadlineTimeout,
		nil,
	}
	for _, okError := range okErrors {
		if errors.Is(err, okError) {
			return false
		}
	}
	return true
}

// ServiceStateTracker wraps the actual service type/interface to track the state of the service.
type ServiceStateTracker struct {
	ServiceRunnerAware
	mu     sync.RWMutex
	state  serviceState
	logger *slog.Logger
}

func newServiceStateTracker(s ServiceRunnerAware, logger *slog.Logger) *ServiceStateTracker {
	switch svc := s.(type) {
	// Need to check whether the type is already service state tracker and assign the correct state and logger.
	case *ServiceStateTracker:
		svc.mu.Lock()
		state := svc.state
		svc.mu.Unlock()
		// This is a bad state and cannot be accepted. If this happen then this surely a bug in the runner.
		if state != serviceStateStopped {
			panic("service is not in stopped state when registered")
		}

		svc.state = serviceStateStopped
		svc.logger = logger
		return svc
	}

	return &ServiceStateTracker{
		ServiceRunnerAware: s,
		state:              serviceStateStopped,
		logger:             logger,
	}
}

func (s *ServiceStateTracker) Name() string {
	return s.ServiceRunnerAware.Name()
}

func (s *ServiceStateTracker) State() serviceState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *ServiceStateTracker) Init(ctx Context) error {
	s.setState(serviceStateInitiating)
	s.logger.Info(fmt.Sprintf("[Service] %s: %s...", s.Name(), s.State()))
	err := s.ServiceRunnerAware.Init(ctx)
	if err != nil {
		return err
	}
	s.setState(serviceStateInitiated)
	s.logger.Info(fmt.Sprintf("[Service] %s: %s", s.Name(), s.State()))
	return err
}

func (s *ServiceStateTracker) Run(ctx context.Context) error {
	if s.getState() >= serviceStateStarting && s.getState() <= serviceStateRunning {
		return errServiceRunnerAlreadyRunning
	}
	s.setState(serviceStateStarting)
	s.logger.Info(fmt.Sprintf("[Service] %s: %s...", s.Name(), s.State()))
	err := s.ServiceRunnerAware.Run(ctx)
	return err
}

func (s *ServiceStateTracker) Ready(ctx context.Context) error {
	if s.getState() >= serviceStateShutdown {
		return errCantCheckReadiness
	}
	if s.getState() == serviceStateRunning {
		return nil
	}
	err := s.ServiceRunnerAware.Ready(ctx)
	if err != nil {
		return err
	}
	if s.getState() != serviceStateRunning {
		s.setState(serviceStateRunning)
	}
	s.logger.Info(fmt.Sprintf("[Service] %s: %s", s.Name(), s.State()))
	return nil
}

// Stop overrides the ServiceRunnerAware stop to ensure we tracked the state of the service
// inside the tracker object.
func (s *ServiceStateTracker) Stop(ctx context.Context) error {
	if s.getState() == serviceStateStopped {
		return nil
	}
	if s.getState() == serviceStateShutdown {
		return errors.New("service is in shutting down state")
	}
	s.setState(serviceStateShutdown)
	s.logger.Info(fmt.Sprintf("[Service] %s: %s...", s.Name(), s.State()))
	err := s.ServiceRunnerAware.Stop(ctx)
	s.setState(serviceStateStopped)
	s.logger.Info(fmt.Sprintf("[Service] %s: %s", s.Name(), s.State()))
	return err
}

func (s *ServiceStateTracker) setState(state serviceState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

func (s *ServiceStateTracker) getState() serviceState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

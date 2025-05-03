package srun

import (
	"errors"
	"os"
	"runtime"
	"time"
)

type Config struct {
	// Name defines the service name and the pid file name.
	Name string
	// Version defines the version of the application.
	Version     string
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
	if c.Name == "" {
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

	if err := c.Logger.Validate(); err != nil {
		return err
	}
	goVersion := runtime.Version()

	// Inject additonal information to the logger for the log fields.
	c.Logger.appName = c.Name
	c.Logger.appVersion = c.Version
	c.Logger.goVersion = goVersion

	if err := c.OtelTracer.Validate(); err != nil {
		return err
	}
	// Inject information to the otel trace configuration
	c.OtelTracer.appName = c.Name
	c.OtelTracer.appVersion = c.Version
	c.OtelTracer.goVersion = goVersion

	if err := c.OtelMetric.Validate(); err != nil {
		return err
	}
	// Inject information to the otel metric configuration
	c.OtelMetric.appName = c.Name
	c.OtelMetric.appVersion = c.Version
	c.OtelMetric.goVersion = goVersion

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

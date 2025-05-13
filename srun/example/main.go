package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"

	"github.com/studio-asd/pkg/resources"
	"github.com/studio-asd/pkg/srun"
)

type Config struct {
	Resources resources.Config `yaml:"resources"`
}

func main() {
	// srun.New().MustRun() wraps the main function and ensure everything is wrapped inside srun scope.
	srun.New(srun.Config{
		Name: "testing",
		Healthcheck: srun.HealthcheckConfig{
			// Please enable the healthcheck if you are aware of what you are doing. The healthcheck service will occupied some of your resources
			// to do active and passive healthcheck as it will spawn a new service and runs a long running goroutine.
			Enabled: true,
		},
		Upgrader: srun.UpgraderConfig{
			// Set the self upgrade to true to enable binary upgrade via SIGHUP(1).
			SelfUpgrade: true,
		},
		Logger: srun.LoggerConfig{
			Format: "json",
			Level:  slog.LevelInfo,
		},
	}).MustRun(run())
}

// run function runs inside srun ensures all errors/operations are encapsulated within srun. Thus, if panic happens, the log of panic will also follows
// the slog log configuration. This especially useful for centralized logging platform as multi-line logs are usually hard to read and need to be combined.
func run() func(context.Context, srun.ServiceRunner) error {
	var configFile string
	var config Config

	flag.Parse()
	flag.StringVar(&configFile, "config", "", "-config=path/to/config/file")
	if configFile == "" {
		return srun.Error(errors.New("config file cannot be empty"))
	}

	return func(ctx context.Context, sr srun.ServiceRunner) error {
		r, err := resources.New(ctx, config.Resources)
		if err != nil {
			return err
		}
		return sr.Register(
			srun.RegisterRunnerServices(r),
		)
	}
}

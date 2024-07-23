package main

import (
	"context"
	"log/slog"

	"github.com/albertwidi/pkg/resources"
	"github.com/albertwidi/pkg/srun"
)

type Config struct {
	Resources resources.Config `yaml:"resources"`
}

func main() {
	srun.New(srun.Config{
		ServiceName: "testing",
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
	}).MustRun(run(Config{}))
}

func run(config Config) func(context.Context, srun.ServiceRunner) error {
	return func(ctx context.Context, sr srun.ServiceRunner) error {
		r, err := resources.New(config.Resources)
		if err != nil {
			return err
		}
		return sr.Register(r)
	}
}

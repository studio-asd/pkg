package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/studio-asd/pkg/srun"
)

func main() {
	srun.New(srun.Config{
		Name: "testing",
		Healthcheck: srun.HealthcheckConfig{
			Enabled: false,
		},
		Admin: srun.AdminConfig{
			Disable: true,
		},
		Upgrader: srun.UpgraderConfig{
			SelfUpgrade: true,
		},
		Logger: srun.LoggerConfig{
			Format: "json",
			Level:  slog.LevelInfo,
		},
	}).MustRun(run())
}

func run() func(context.Context, srun.ServiceRunner) error {
	return func(ctx context.Context, sr srun.ServiceRunner) error {
		lrt, err := srun.NewLongRunningTask("http-server", func(ctx srun.Context) error {
			httpServer := &http.Server{
				Addr: "localhost:8080",
			}
			return httpServer.ListenAndServe()
		})
		if err != nil {
			return err
		}
		return sr.Register(
			srun.RegisterRunnerAwareServices(lrt),
		)
	}
}

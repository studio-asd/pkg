package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/studio-asd/pkg/srun"
)

func main() {
	r := srun.New(srun.Config{
		Name:    "testing",
		Version: "0.1",
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
			Format:                  "text",
			Level:                   slog.LevelInfo,
			RemoveTime:              true,
			RemoveDefaultAttributes: true,
		},
	})
	r.SetTestConfigFunc(func(ctx srun.Context) error {
		return errors.New("in test config func")
	})
	r.MustRun(run())
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

package resources_test

import (
	"log/slog"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/studio-asd/pkg/resources"
	grpcserver "github.com/studio-asd/pkg/resources/grpc/server"
	"github.com/studio-asd/pkg/resources/postgres"
	"github.com/studio-asd/pkg/srun"
)

func TestResources(t *testing.T) {
	yamlConfig := `
grpc:
  servers:
    - name: "test_resources"
      address: ":20010"
      read_timeout: "30s"
      write_timeout: "30s"
      grpc_gateway:
        address: ":8090"

postgres:
  monitor_stats: true
  max_retry: 3
  retry_delay: "1s"
  connects:
    - name: "go_example"
      driver: "pgx"
      primary:
        dsn: "postgres://postgres:postgres@127.0.0.1:5432/?sslmode=disable"
`

	config := resources.Config{}
	if err := yaml.Unmarshal([]byte(yamlConfig), &config); err != nil {
		t.Fatal(err)
	}

	r, err := resources.New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Init(srun.Context{
		Logger: slog.Default(),
	}); err != nil {
		t.Fatal(err)
	}

	var errC = make(chan error, 1)
	go func() {
		errC <- r.Run(t.Context())
	}()

	// Wait for 300ms to check whether there is an error or not.
	afterC := time.After(time.Millisecond * 300)
	select {
	case err := <-errC:
		t.Fatal(err)
	case <-afterC:
		break
	}

	_, err = resources.Get[postgres.PostgresDB](r.Container(), "go_example")
	if err != nil {
		t.Fatal(err)
	}
	_, err = resources.Get[*grpcserver.GRPCServer](r.Container(), "test_resources")
	if err != nil {
		t.Fatal(err)
	}
}

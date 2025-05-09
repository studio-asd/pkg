package resources_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/studio-asd/pkg/resources"
	grpcserver "github.com/studio-asd/pkg/resources/grpc/server"
	"github.com/studio-asd/pkg/resources/postgres"
	"github.com/studio-asd/pkg/srun"
)

func TestResources(t *testing.T) {
	out, err := os.ReadFile("./testdata/testconfig.yaml")
	if err != nil {
		t.Fatal(err)
	}

	config := resources.Config{}
	if err := yaml.Unmarshal(out, &config); err != nil {
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

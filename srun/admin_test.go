package srun

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestAdminServerConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *AdminServerConfig
		expect *AdminServerConfig
	}{
		{
			name: "default configuration",
			config: &AdminServerConfig{
				Address: ":8080",
			},
			expect: &AdminServerConfig{
				Address:          ":8080",
				HTTPServerConfig: adminHTTPServerDefaultConfig,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.config.validate(); err != nil {
				t.Fatal(err)
			}
			if test.expect.HealthcheckFunc == nil {
				test.expect.HealthcheckFunc = test.config.HealthcheckFunc
			}
			if test.expect.ReadinessFunc == nil {
				test.expect.ReadinessFunc = test.config.ReadinessFunc
			}
			opts := []cmp.Option{
				cmpopts.IgnoreUnexported(AdminServerConfig{}),
				cmpopts.IgnoreFields(AdminServerConfig{}, "HealthcheckFunc", "ReadinessFunc"),
			}
			if diff := cmp.Diff(test.config, test.expect, opts...); diff != "" {
				t.Fatalf("(-want/+got)\n%s", diff)
			}
		})
	}
}

// TestAdminEndpoints tests all endpoints provided by the admin server and whether the endpoint returns 200(OK).
func TestAdminEndpoints(t *testing.T) {
	t.Parallel()

	endpoints := []string{
		"/metrics",
		"/debug/pprof",
		"/debug/cmdline",
		"/debug/symbol",
		"/debug/trace",
		"/debug/allocs",
		"/debug/block",
		"/debug/heap",
		"/debug/mutex",
		"/debug/threadcreate",
	}

	// Use the default configuration, so it will start serving on :8778.
	admin, err := newAdminServer(AdminServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Init(Context{}); err != nil {
		t.Fatal(err)
	}

	errC := make(chan error, 1)
	go func() {
		errC <- admin.Run(context.Background())
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	select {
	case err := <-errC:
		t.Fatal(err)
	case <-ticker.C:
		ticker.Stop()
		break
	}
	defer admin.Stop(context.Background())

	client := http.Client{}
	defer client.CloseIdleConnections()

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			url, err := url.JoinPath("http://localhost:8778", endpoint)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expecting code 200(OK) but got %d", resp.StatusCode)
			}
		})
	}
}

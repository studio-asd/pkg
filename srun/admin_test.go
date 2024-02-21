package srun

import (
	"testing"

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

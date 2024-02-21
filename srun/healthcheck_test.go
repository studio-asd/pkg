package srun

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestHealthcheckRegister(t *testing.T) {
	tests := []struct {
		name     string
		config   HealthcheckConfig
		services func() []ServiceRunnerAware
		expect   func(HealthcheckConfig, ...ServiceRunnerAware) *HealthcheckService
	}{
		{
			name: "long running task",
			services: func() []ServiceRunnerAware {
				lrt, _ := NewLongRunningTask("test-lrt", func(ctx context.Context) error { return nil })
				return []ServiceRunnerAware{lrt}
			},
			expect: func(config HealthcheckConfig, services ...ServiceRunnerAware) *HealthcheckService {
				hcs := newHealthcheckService(config)
				for _, svc := range services {
					hcs.notifiers[svc] = &HealthcheckNotifier{
						serviceName: svc.Name(),
						noop:        config.Disable,
						notifyC:     hcs.notifC,
					}
				}
				return hcs
			},
		},
		{
			name: "concurrent service",
			services: func() []ServiceRunnerAware {
				lrt1, _ := NewLongRunningTask("test-lrt-1", func(ctx context.Context) error { return nil })
				lrt2, _ := NewLongRunningTask("test-lrt-2", func(ctx context.Context) error { return nil })
				lrt3, _ := NewLongRunningTask("test-lrt-3", func(ctx context.Context) error { return nil })
				cr, err := newConcurrentServices(nil, lrt1, lrt2, lrt3)
				if err != nil {
					panic(err)
				}
				return []ServiceRunnerAware{cr}
			},
			expect: func(config HealthcheckConfig, services ...ServiceRunnerAware) *HealthcheckService {
				// We will only expect one service, which is a concurrent service.
				cs := services[0].(*ConcurrentServices)
				hcs := newHealthcheckService(config)
				for _, svc := range cs.services {
					hcs.notifiers[svc.ServiceRunnerAware] = &HealthcheckNotifier{
						serviceName: svc.Name(),
						noop:        config.Disable,
						notifyC:     hcs.notifC,
					}
				}
				return hcs
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hcs := newHealthcheckService(test.config)
			services := test.services()
			for _, svc := range services {
				if err := hcs.register(svc); err != nil {
					t.Fatal(err)
				}
			}
			expect := test.expect(test.config, services...)

			// Replace the expect.notifyC with hcs.NotifyC. Unless we do this the channel will be different because they are
			// different instance of healthcheck service.
			for svc, hcn := range hcs.notifiers {
				t.Logf("remap notifyC for service %s", svc.Name())
				expect.notifiers[svc].notifyC = hcn.notifyC
			}
			// Configuration diff.
			if diff := cmp.Diff(expect.config, hcs.config); diff != "" {
				t.Fatalf("(-wan/+got) Config:\n%s", diff)
			}
			// Notifiers diff.
			if diff := cmp.Diff(
				expect.notifiers, hcs.notifiers,
				cmpopts.IgnoreFields(HealthcheckNotifier{}, "notifyC"),
				cmpopts.IgnoreInterfaces(struct{ ServiceRunnerAware }{}),
				cmpopts.EquateComparable(HealthcheckNotifier{}),
			); diff != "" {
				t.Fatalf("(-wan/+got) Notifiers:\n%s", diff)
			}
		})
	}
}

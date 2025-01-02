package srun

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestHealthcheckRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   HealthcheckConfig
		services func() []ServiceRunnerAware
		expect   func(HealthcheckConfig, ...ServiceRunnerAware) *HealthcheckService
	}{
		{
			name: "long running task",
			config: HealthcheckConfig{
				Enabled: true,
			},
			services: func() []ServiceRunnerAware {
				lrt, _ := NewLongRunningTask("test-lrt", func(ctx Context) error { return nil })
				return []ServiceRunnerAware{lrt}
			},
			expect: func(config HealthcheckConfig, services ...ServiceRunnerAware) *HealthcheckService {
				hcs := newHealthcheckService(config)
				for _, svc := range services {
					hcs.notifiers[svc] = &HealthcheckNotifier{
						serviceName: svc.Name(),
						noop:        !config.Enabled,
						notifyC:     hcs.notifC,
					}
				}
				return hcs
			},
		},
		{
			name: "concurrent service",
			config: HealthcheckConfig{
				Enabled: true,
			},
			services: func() []ServiceRunnerAware {
				lrt1, _ := NewLongRunningTask("test-lrt-1", func(ctx Context) error { return nil })
				lrt2, _ := NewLongRunningTask("test-lrt-2", func(ctx Context) error { return nil })
				lrt3, _ := NewLongRunningTask("test-lrt-3", func(ctx Context) error { return nil })
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
					hcs.notifiers[svc.ServiceInitAware] = &HealthcheckNotifier{
						serviceName: svc.Name(),
						noop:        !config.Enabled,
						notifyC:     hcs.notifC,
					}
				}
				return hcs
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
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

type TestHealthcheckNotif struct{}

func (*TestHealthcheckNotif) Name() string {
	return "test-notif"
}

func (*TestHealthcheckNotif) Init(ctx Context) error {
	return nil
}

func (*TestHealthcheckNotif) Run(ctx context.Context) error {
	return nil
}

func (*TestHealthcheckNotif) Ready(ctx context.Context) error {
	return nil
}

func (*TestHealthcheckNotif) Stop(ctx context.Context) error {
	return nil
}

func (t *TestHealthcheckNotif) ConsumeHealthcheckNotification(fn HealthcheckNotifyFunc) {
	err := fn([]string{"test_svc"}, func(notif HealthcheckNotification) {
	})
	fmt.Print(err)
}

func TestHandleNotifications(t *testing.T) {
	t.Parallel()
	hs := newHealthcheckService(HealthcheckConfig{Enabled: true})
	if err := hs.Init(Context{}); err != nil {
		t.Fatal(err)
	}
	if err := hs.register(&TestHealthcheckNotif{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go hs.handleNotifications(ctx)
	hs.notifC <- HealthcheckNotification{
		ServiceName: "test_svc",
	}
	cancel()
}

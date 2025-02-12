package resources

import (
	"context"
	"sync"

	"github.com/studio-asd/pkg/srun"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var _ srun.ServiceRunnerAware = (*Resources)(nil)

type Config struct {
	Postgres *PostgresResourcesConfig `yaml:"postgres"`
}

func (c Config) Validate() error {
	if err := c.Postgres.Validate(); err != nil {
		return err
	}
	return nil
}

type Resources struct {
	config Config

	// OpenTelemetry tracer and metric meter.
	tracer trace.Tracer
	meter  metric.Meter

	mu    sync.Mutex
	stopC chan struct{}

	// PostgreSQL connections.
	postrgres *postgresResources
}

func New(ctx context.Context, config Config) (*Resources, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	r := &Resources{
		config: config,
		stopC:  make(chan struct{}, 1),
	}
	if err := r.init(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Resources) Name() string {
	return "resources"
}

func (r *Resources) Init(ctx srun.Context) error {
	r.tracer = ctx.Tracer
	r.meter = ctx.Meter
	return r.init(ctx.Ctx)
}

func (r *Resources) init(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.postrgres != nil {
		resources, err := r.config.Postgres.connect(ctx)
		if err != nil {
			return err
		}
		r.postrgres = resources
	}
	return nil
}

func (r *Resources) Run(ctx context.Context) error {
	<-r.stopC
	return nil
}

func (r *Resources) Ready(ctx context.Context) error {
	return nil
}

func (r *Resources) Stop(ctx context.Context) error {
	if r.postrgres != nil {
		if err := r.postrgres.close(); err != nil {
			return err
		}
	}
	r.stopC <- struct{}{}
	return nil
}

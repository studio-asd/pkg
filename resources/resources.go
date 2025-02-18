package resources

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/studio-asd/pkg/srun"
)

var _ srun.ServiceRunnerAware = (*Resources)(nil)

type Config struct {
	Postgres *PostgresResourcesConfig `yaml:"postgres"`
	GRPC     *GRPCResourcesConfig     `yaml:"grpc"`
}

func (c Config) Validate() error {
	if c.Postgres != nil {
		if err := c.Postgres.Validate(); err != nil {
			return err
		}
	}
	if c.GRPC != nil {
		if err := c.GRPC.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type Resources struct {
	config Config
	logger *slog.Logger
	// OpenTelemetry tracer and metric meter.
	tracer trace.Tracer
	meter  metric.Meter

	mu    sync.Mutex
	stopC chan struct{}

	// PostgreSQL connections.
	postrgres *postgresResources
	// GRPC client connections.
	grpc *grpcResources
}

func New(ctx context.Context, config Config) (*Resources, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	r := &Resources{
		config: config,
		stopC:  make(chan struct{}, 1),
	}
	return r, nil
}

func (r *Resources) Name() string {
	return "resources"
}

func (r *Resources) Init(ctx srun.Context) error {
	r.logger = ctx.Logger
	r.tracer = ctx.Tracer
	r.meter = ctx.Meter
	// Inject the logger for all the resources.
	r.config.Postgres.logger = r.logger
	r.config.GRPC.logger = r.logger
	return nil
}

func (r *Resources) Run(ctx context.Context) error {
	err := r.run(ctx)
	<-r.stopC
	return err
}

func (r *Resources) Ready(ctx context.Context) error {
	return nil
}

func (r *Resources) Stop(ctx context.Context) error {
	var errs error
	if r.postrgres != nil {
		r.logger.Info("[resources] closing all PostgreSQL connections")
		if err := r.postrgres.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if r.grpc != nil {
		r.logger.Info("[resources] closing all gRPC connections")
		if err := r.grpc.clientResources.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	r.stopC <- struct{}{}
	return nil
}

func (r *Resources) run(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// PostgreSQL.
	if r.config.Postgres != nil {
		r.logger.Info(
			"[resources] Found postgres configuration, establishing connection to PostgreSQL databases...",
			slog.Int("postgres_config_count", len(r.config.Postgres.PostgresConnections)),
		)
		pgResources, err := r.config.Postgres.connect(ctx)
		if err != nil {
			return err
		}
		r.postrgres = pgResources
	}
	// GRPC.
	if r.config.GRPC != nil {
		// GRPC Clients.
		if len(r.config.GRPC.ClientResources) > 0 {
			r.logger.Info(
				"[resources] Found grpc clients configuration, establishing connection to gRPC endpoints...",
				slog.Int("grpc_clients_config_count", len(r.config.GRPC.ClientResources)),
			)
		}
		grpcResources, err := r.config.GRPC.connect(ctx)
		if err != nil {
			return err
		}
		r.grpc = grpcResources
	}
	return nil
}

package resources

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/studio-asd/pkg/srun"
)

var _ srun.ServiceRunnerAware = (*Resources)(nil)

type Config struct {
	Postgres *PostgresResourcesConfig `yaml:"postgres"`
	Redis    *RedisResourcesConfig    `yaml:"redis"`
	GRPC     *GRPCResourcesConfig     `yaml:"grpc"`
}

func (c *Config) Validate() error {
	// nonil flags that at least one(1) configurations is not empty. Please set the nonil to true for every time
	// we check a configuration.
	var nonil bool

	if c.Postgres != nil {
		nonil = true
		if err := c.Postgres.Validate(); err != nil {
			return err
		}
	}
	if c.GRPC != nil {
		nonil = true
		if err := c.GRPC.Validate(); err != nil {
			return err
		}
	}

	if !nonil {
		return errors.New("resources: empty configuration")
	}
	return nil
}

type Resources struct {
	config Config
	logger *slog.Logger
	// OpenTelemetry tracer and metric meter.
	tracer trace.Tracer
	meter  metric.Meter
	// container stores all the resources defined in this library.
	container *ResourcesContainer

	stateHelper *srun.StateHelper
	stopC       chan struct{}
}

type ResourcesContainer struct {
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
		config:    config,
		stopC:     make(chan struct{}, 1),
		container: &ResourcesContainer{},
	}
	return r, nil
}

// Container returns the resources container that contains all the available/connected resources. Please note that
// all resources are only available after Run() is invoked.
func (r *Resources) Container() *ResourcesContainer {
	return r.container
}

func (r *Resources) Name() string {
	return "resources"
}

func (r *Resources) Init(ctx srun.Context) error {
	r.logger = ctx.Logger
	r.tracer = ctx.Tracer
	r.meter = ctx.Meter

	if r.stateHelper == nil {
		r.stateHelper = ctx.NewStateHelper("resources")
		r.stateHelper.SetInitiated()
	}
	// Don't re-init because we don't want to reconnect to all dependencies all over again.
	if r.stateHelper.IsInitiated() {
		return nil
	}

	// Inject the logger for all the resources.
	if r.config.Postgres != nil {
		r.config.Postgres.logger = r.logger
	}
	if r.config.GRPC != nil {
		r.config.GRPC.logger = r.logger
	}

	if err := r.init(ctx.Ctx); err != nil {
		return err
	}
	return nil
}

func (r *Resources) Run(ctx context.Context) error {
	if r.stateHelper.IsRunning() {
		return nil
	}
	r.stateHelper.SetRunning()

	<-r.stopC
	return nil
}

func (r *Resources) Ready(ctx context.Context) error {
	if !r.stateHelper.IsRunning() {
		return errors.New("the resources are stopped")
	}
	return nil
}

func (r *Resources) Stop(ctx context.Context) error {
	// Early return when the resources is not currently running.
	if r.stateHelper.IsStopped() {
		return nil
	}

	var errs error
	if r.container.postrgres != nil {
		r.logger.Info("[resources] closing all PostgreSQL connections")
		if err := r.container.postrgres.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if r.container.grpc != nil {
		r.logger.Info("[resources] closing all gRPC connections")
		if err := r.container.grpc.clientResources.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	r.stopC <- struct{}{}
	return errs
}

// init initiates some of the resources that need to be created before the object is registered to the srun. This is because
// some of the resources might be needed in the time of initialization. For example, both HTTP and gRPC servers need handler
// to works. Other thing is, databases and other objects that needed in Init phase are also need to be initiateed.
func (r *Resources) init(ctx context.Context) error {
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
		r.container.postrgres = pgResources
	}
	// GRPC.
	if r.config.GRPC != nil {
		grpcResources := newGRPCResources()

		// GRPC Clients.
		if len(r.config.GRPC.ClientResources) > 0 {
			r.logger.Info(
				"[resources] Found grpc clients configuration, establishing connection to gRPC endpoints...",
				slog.Int("grpc_clients_config_count", len(r.config.GRPC.ClientResources)),
			)
			err := r.config.GRPC.connectGRPCClients(ctx, grpcResources)
			if err != nil {
				return err
			}
		}
		// GRPC Servers.
		if len(r.config.GRPC.ServerResources) > 0 {
			r.logger.Info(
				"[resources] Found grpc servers configuration, creating gRPC servers...",
				slog.Int("grpc_servers_config", len(r.config.GRPC.ServerResources)),
			)
			err := r.config.GRPC.createGRPCServers(ctx, grpcResources)
			if err != nil {
				return err
			}
		}

		r.container.grpc = grpcResources
	}
	return nil
}

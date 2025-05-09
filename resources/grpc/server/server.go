package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	grpcserver "github.com/studio-asd/pkg/grpc/server"
	"github.com/studio-asd/pkg/resources"
	"github.com/studio-asd/pkg/srun"
)

var _ resources.GRPCServerPackage = (*GRPCServerLib)(nil)

func init() {
	lib := &GRPCServerLib{
		servers: make(map[string]*GRPCServer),
		configs: make(map[string]resources.GRPCServerResourceConfig),
	}
	resources.RegisterPackage(lib)
}

type GRPCServerLib struct {
	logger  *slog.Logger
	servers map[string]*GRPCServer
	configs map[string]resources.GRPCServerResourceConfig
}

func (g *GRPCServerLib) SetLogger(logger *slog.Logger) {
	g.logger = logger.With(slog.String("logger.scope", "grpc-server-lib"))
}

func (s *GRPCServerLib) Init(ctx srun.Context) error {
	for _, server := range s.servers {
		if server.server != nil {
			if err := server.server.Init(srun.Context{
				RunnerAppName:    ctx.RunnerAppName,
				RunnerAppVersion: ctx.RunnerAppVersion,
				Ctx:              ctx.Ctx,
				Logger:           ctx.Logger,
				Meter:            otel.GetMeterProvider().Meter("grpc-server"),
				Tracer:           otel.GetTracerProvider().Tracer("grpc-server"),
				Flags:            ctx.Flags,
			}); err != nil {
				return err
			}
		}
		if server.gateway != nil {
			if err := server.gateway.init(srun.Context{
				RunnerAppName:    ctx.RunnerAppName,
				RunnerAppVersion: ctx.RunnerAppVersion,
				Ctx:              ctx.Ctx,
				Logger:           ctx.Logger,
				Meter:            otel.GetMeterProvider().Meter("grpc-gateway-server"),
				Tracer:           otel.GetTracerProvider().Tracer("grpc-gateway-server"),
				Flags:            ctx.Flags,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *GRPCServerLib) Close(ctx context.Context) error {
	var errs error
	for _, server := range g.servers {
		// If the grpc gateway is not nil, then we should ALWAYS stop the grpc gateway first before closing the grpc server. This is because we might
		// want to change the implementation to a proxy model latern on, and when this happen the grpc server cannot be closed before the gateway
		// is closed.
		if server.gateway != nil {
			err := server.gateway.stop(ctx)
			if err != nil {
				errs = errors.Join(errs, err)
			}
		}
		if server.server != nil {
			err := server.server.Stop(ctx)
			if err != nil {
				errs = errors.Join(errs, err)
			}
		}
	}
	return errs
}

func (g *GRPCServerLib) Create(ctx context.Context, confs []resources.GRPCServerResourceConfig) (map[string]any, error) {
	servers := make(map[string]any)
	for _, conf := range confs {
		server, err := g.createGRPCServer(ctx, conf)
		if err != nil {
			return nil, err
		}
		servers[conf.Name] = server
		g.servers[conf.Name] = server
		g.configs[conf.Name] = conf
	}
	return servers, nil
}

func (g *GRPCServerLib) Run(ctx context.Context) error {
	// By using the errgroup it doesn't guarantee that the grpc server is running first before the grpc gateway. While its not important now
	// we might want to revisit later to ensure the grpc server is running first before the grpc gateway.
	eg := errgroup.Group{}
	for name, server := range g.servers {
		if server.server != nil {
			g.logger.LogAttrs(
				ctx,
				slog.LevelInfo,
				"[resources][grpc_server] starting gRPC server...",
				slog.String("grpc_server.name", name),
				slog.String("grpc_server.address", g.configs[name].Address),
			)
			eg.Go(func() error {
				err := server.server.Run(ctx)
				if err != nil {
					g.logger.LogAttrs(
						ctx,
						slog.LevelError,
						"[resources][grpc_server] shutting down gRPC server with error",
						slog.String("grpc_server.name", name),
						slog.String("grpc_server.address", g.configs[name].Address),
						slog.String("error", err.Error()),
					)
				}
				return err
			})
		}
		if server.gateway != nil {
			// Add a wait time if the grpc server is not nil to give some time to the scheduler to schedule the goroutine.
			if server.server != nil {
				time.Sleep(time.Millisecond * 100)
			}
			g.logger.LogAttrs(
				ctx,
				slog.LevelInfo,
				"[resources][grpc_server] starting gRPC Gateway server...",
				slog.String("grpc_gateway_server.name", g.configs[name].GRPCGateway.Name),
				slog.String("grpc_server.address", g.configs[name].GRPCGateway.Address),
			)
			eg.Go(func() error {
				err := server.gateway.run(ctx)
				if err != nil {
					g.logger.LogAttrs(
						ctx,
						slog.LevelError,
						"[resources][grpc_server] shutting down gRPC Gateway server with error",
						slog.String("grpc_gateway_server.name", g.configs[name].GRPCGateway.Name),
						slog.String("grpc_server.address", g.configs[name].GRPCGateway.Address),
						slog.String("error", err.Error()),
					)
				}
				return err
			})
		}
	}
	return eg.Wait()
}

func (g *GRPCServerLib) createGRPCServer(ctx context.Context, conf resources.GRPCServerResourceConfig) (*GRPCServer, error) {
	server, err := grpcserver.New(grpcserver.Config{
		Address:      conf.Address,
		WriteTimeout: time.Duration(conf.WriteTimeout),
		ReadTimeout:  time.Duration(conf.ReadTimeout),
	})
	if err != nil {
		return nil, err
	}
	grpcServer := &GRPCServer{
		address: conf.Address,
		server:  server,
	}
	// The address of grpc gateway is not empty, we should create the grpc gateway object for this specific
	// grpc server.
	if conf.GRPCGateway.Address != "" {
		gateway, err := g.createGRPCGateway(ctx, conf.GRPCGateway)
		if err != nil {
			return nil, err
		}
		grpcServer.gateway = gateway
	}
	return grpcServer, nil
}

type GRPCServer struct {
	address string
	server  *grpcserver.Server
	gateway *GRPCGateway
}

// RegisterService registers the grpc services to the grpc server.
func (g *GRPCServer) RegisterService(fn func(s grpc.ServiceRegistrar)) {
	g.server.RegisterService(fn)
}

func (g *GRPCServer) RegisterGatewayService(fn func(gateway *GRPCGateway) error) error {
	if err := fn(g.gateway); err != nil {
		return err
	}
	return nil
}

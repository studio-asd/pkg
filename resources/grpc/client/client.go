package client

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/studio-asd/pkg/grpc/client"
	"github.com/studio-asd/pkg/resources"
	"github.com/studio-asd/pkg/srun"
)

var _ resources.GRPCClientPackage = (*GRPCCLientLib)(nil)

func init() {
	lib := &GRPCCLientLib{
		conns: make(map[string]*grpc.ClientConn),
	}
	resources.RegisterPackage(lib)
}

type GRPCCLientLib struct {
	logger *slog.Logger
	conns  map[string]*grpc.ClientConn
}

func (g *GRPCCLientLib) SetLogger(logger *slog.Logger) {
	g.logger = logger.With(slog.String("logger.scope", "grpc-client-lib"))
}

func (g *GRPCCLientLib) Init(ctx srun.Context) error {
	return nil
}

func (g *GRPCCLientLib) Close(ctx context.Context) error {
	var errs error
	for name, conn := range g.conns {
		attrs := []slog.Attr{
			slog.String("grpc_client.name", name),
			slog.String("grpc_client.address", conn.Target()),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_client] closing connection to gRPC client...",
			attrs...,
		)
		if err := conn.Close(); err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
			g.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][grpc_client] failed to close connection to gRPC client",
				attrs...,
			)
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (g *GRPCCLientLib) Connect(ctx context.Context, confs []resources.GRPCClientResourceConfig) (map[string]any, error) {
	conns := make(map[string]any)
	for _, conf := range confs {
		attrs := []slog.Attr{
			slog.String("grpc_client.name", conf.Name),
			slog.String("grpc_client.address", conf.Address),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_client] connecting to gRPC client...",
			attrs...,
		)
		c, err := client.New(
			context.Background(),
			client.Config{
				Address: conf.Address,
			},
		)
		if err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
			g.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][grpc_client] failed to connect",
				attrs...,
			)
			return nil, err
		}
		conns[conf.Name] = c
		g.conns[conf.Name] = c
	}
	return conns, nil
}

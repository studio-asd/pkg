package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/grpc"

	"github.com/studio-asd/pkg/grpc/client"
)

func newGRPCResources() *grpcResources {
	return &grpcResources{
		clientResources: &grpcClientResources{
			clients: make(map[string]*grpc.ClientConn),
		},
	}
}

type grpcResources struct {
	clientResources *grpcClientResources
}

type grpcClientResources struct {
	mu      sync.Mutex
	clients map[string]*grpc.ClientConn
}

func (g *grpcClientResources) setClient(name string, conn *grpc.ClientConn) {
	g.mu.Lock()
	g.clients[name] = conn
	g.mu.Unlock()
}

func (g *grpcClientResources) getClient(name string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	conn, ok := g.clients[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_client: client with name %s not exist", name)
	}
	return conn, nil
}

func (g *grpcClientResources) close() error {
	var errs error
	for _, c := range g.clients {
		err := c.Close()
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

type GRPCResourcesConfig struct {
	logger          *slog.Logger
	ClientResources []GRPCClientResourceConfig `yaml:"clients"`
}

func (g *GRPCResourcesConfig) Validate() error {
	for idx, c := range g.ClientResources {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("grpc_resources/client [%d]: %w", idx, err)
		}
	}
	return nil
}

func (g *GRPCResourcesConfig) connect(ctx context.Context) (*grpcResources, error) {
	resources := newGRPCResources()
	for _, c := range g.ClientResources {
		attrs := []slog.Attr{
			slog.String("client.name", c.Name),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_clients] connecting to GRPC endpoint",
			attrs...,
		)
		conn, err := c.Connect()
		if err != nil {
			g.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][grpc_clients] failed to connect to GRPC endpoint",
				attrs...,
			)
			return nil, err
		}
		resources.clientResources.setClient(c.Name, conn)
	}
	return resources, nil
}

type GRPCClientResourceConfig struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

func (g *GRPCClientResourceConfig) Validate() error {
	if g.Name == "" {
		return errors.New("grpc_client: name cannot be empty, this will be used as key identifier")
	}
	if g.Address == "" {
		return fmt.Errorf("grpc_client [%s]: address cannot be empty", g.Name)
	}
	return nil
}

func (g *GRPCClientResourceConfig) Connect() (*grpc.ClientConn, error) {
	c, err := client.New(
		context.Background(),
		client.Config{
			Address: g.Address,
		},
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

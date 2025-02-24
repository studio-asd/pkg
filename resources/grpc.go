package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	"github.com/studio-asd/pkg/grpc/client"
	grpcserver "github.com/studio-asd/pkg/grpc/server"
	httpserver "github.com/studio-asd/pkg/http/server"
)

const (
	defaultGRPCServerReadTimeout  = time.Second * 30
	defaultGRPCServerWriteTimeout = time.Second * 20
)

func newGRPCResources() *grpcResources {
	return &grpcResources{
		clientResources: &grpcClientResources{
			clients: make(map[string]*grpc.ClientConn),
		},
	}
}

type grpcResources struct {
	clientResources  *grpcClientResources
	severResources   *grpcServerResources
	gatewayResources *grpcGatewayResources
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

type grpcServerResources struct {
	mu      sync.Mutex
	servers map[string]*grpcserver.Server
}

func (g *grpcServerResources) setServer(name string, server *grpcserver.Server) {
	g.mu.Lock()
	g.servers[name] = server
	g.mu.Unlock()
}

func (g *grpcServerResources) getServer(name string) (*grpcserver.Server, error) {
	g.mu.Lock()
	server, ok := g.servers[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_server: server with name %s does not exist", name)
	}
	return server, nil
}

type grpcGatewayResources struct {
	mu sync.Mutex
}

type GRPCResourcesConfig struct {
	logger          *slog.Logger
	ServerResources []GRPCServerResourceConfig `yaml:"servers"`
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

func (g *GRPCResourcesConfig) connectGRPCClients(ctx context.Context, resources *grpcResources) error {
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
			return err
		}
		resources.clientResources.setClient(c.Name, conn)
	}
	return nil
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

func (g *GRPCResourcesConfig) createGRPCServers(ctx context.Context, resources *grpcResources) error {
	for _, server := range g.ServerResources {
		s, err := server.create()
		if err != nil {
			return err
		}
		resources.severResources.setServer(server.Name, s)
	}
	return nil
}

type GRPCServerResourceConfig struct {
	Name         string   `yaml:"name"`
	Address      string   `yaml:"address"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
	// GRPCGateway configuration allows the grpc server to be proxied through http server as long as the
	// endpoint supports grpc gateway.
	//
	// Read https://github.com/grpc-ecosystem/grpc-gateway to know more about grpc gateway.
	GRPCGateway GRPCGatewayResourceConfig `yaml:"grpc_gateway"`
}

func (g *GRPCServerResourceConfig) validate() error {
	if g.Name == "" {
		return errors.New("grpc_server: name cannot be empty")
	}
	if g.Address == "" {
		return fmt.Errorf("grpc_server [%s]: address cannot be empty", g.Name)
	}
	if g.ReadTimeout == 0 {
		g.ReadTimeout = Duration(defaultGRPCServerReadTimeout)
	}
	if g.WriteTimeout == 0 {
		g.WriteTimeout = Duration(defaultGRPCServerWriteTimeout)
	}
	// Override the write and read timeout according to the write and read timeout of grpc server.
	if g.GRPCGateway.Addres != "" {
		g.GRPCGateway.name = g.Name
		if g.GRPCGateway.writeTimeout == 0 {
			g.GRPCGateway.writeTimeout = g.WriteTimeout
		}
		if g.GRPCGateway.readTimeout == 0 {
			g.GRPCGateway.readTimeout = g.ReadTimeout
		}
	}
	return nil
}

func (g *GRPCServerResourceConfig) create() (*grpcserver.Server, error) {
	server, err := grpcserver.New(grpcserver.Config{
		Address:      g.Address,
		WriteTimeout: time.Duration(g.WriteTimeout),
		ReadTimeout:  time.Duration(g.ReadTimeout),
	})
	if err != nil {
		return nil, err
	}
	return server, nil
}

type GRPCGatewayResourceConfig struct {
	Addres       string `yaml:"address"`
	name         string
	writeTimeout Duration
	readTimeout  Duration
}

type GRPCGatewayObject struct {
	servicesHandlerFn func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error)
	httpServer        *httpserver.Server
}

func (g *GRPCGatewayResourceConfig) create() (*GRPCGatewayObject, error) {
	s, err := httpserver.New(httpserver.Config{
		Name:         fmt.Sprintf("%s-grpc-gateway", g.name),
		WriteTimeout: time.Duration(g.writeTimeout),
		ReadTimeout:  time.Duration(g.readTimeout),
	})
	if err != nil {
		return nil, err
	}
	return &GRPCGatewayObject{
		httpServer: s,
	}, nil
}

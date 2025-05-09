package resources

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultGRPCServerReadTimeout  = time.Second * 30
	defaultGRPCServerWriteTimeout = time.Second * 20
)

type grpcClientResources struct {
	logger  *slog.Logger
	mu      sync.Mutex
	clients map[string]*grpc.ClientConn
}

func (g *grpcClientResources) setClient(name string, conn *grpc.ClientConn) {
	g.mu.Lock()
	g.clients[name] = conn
	g.mu.Unlock()
}

func (g *grpcClientResources) GetClient(name string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	conn, ok := g.clients[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_client: client with name %s not exist", name)
	}
	return conn, nil
}

func (g *grpcClientResources) close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

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
	ServerResources []GRPCServerResourceConfig `yaml:"servers"`
	ClientResources []GRPCClientResourceConfig `yaml:"clients"`
}

func (g *GRPCResourcesConfig) Validate() error {
	g.logger = slog.Default()
	for idx, c := range g.ClientResources {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("grpc_resources/client [%d]: %w", idx, err)
		}
	}
	for idx, s := range g.ServerResources {
		if err := s.validate(); err != nil {
			return fmt.Errorf("grpc_resources/server [%d]: %w", idx, err)
		}
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
	if g.GRPCGateway.Address != "" {
		g.GRPCGateway.Name = g.Name
		if g.GRPCGateway.WriteTimeout == 0 {
			g.GRPCGateway.WriteTimeout = g.WriteTimeout
		}
		if g.GRPCGateway.ReadTimeout == 0 {
			g.GRPCGateway.ReadTimeout = g.ReadTimeout
		}
	}
	return nil
}

type GRPCGatewayResourceConfig struct {
	Address       string   `yaml:"address"`
	Name          string   `yaml:"-"`
	ServerAddress string   `yaml:"-"`
	WriteTimeout  Duration `yaml:"-"`
	ReadTimeout   Duration `yaml:"-"`
}

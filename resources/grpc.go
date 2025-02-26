package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/studio-asd/pkg/grpc/client"
	grpcserver "github.com/studio-asd/pkg/grpc/server"
	httpserver "github.com/studio-asd/pkg/http/server"
	"github.com/studio-asd/pkg/srun"
)

const (
	defaultGRPCServerReadTimeout  = time.Second * 30
	defaultGRPCServerWriteTimeout = time.Second * 20
)

func newGRPCResources(logger *slog.Logger) *grpcResources {
	return &grpcResources{
		Client: &grpcClientResources{
			logger:  logger,
			clients: make(map[string]*grpc.ClientConn),
		},
		Server: &grpcServerResources{
			logger:  logger,
			servers: make(map[string]*grpcserver.Server),
		},
		Gateway: &grpcGatewayResources{
			logger:   logger,
			gateways: make(map[string]*GRPCGatewayObject),
		},
	}
}

type grpcResources struct {
	Client  *grpcClientResources
	Server  *grpcServerResources
	Gateway *grpcGatewayResources
}

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

type grpcServerResources struct {
	logger  *slog.Logger
	mu      sync.Mutex
	servers map[string]*grpcserver.Server
}

func (g *grpcServerResources) isEmpty() bool {
	g.mu.Lock()
	l := len(g.servers)
	g.mu.Unlock()
	return l == 0
}

func (g *grpcServerResources) close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var errs error
	for _, server := range g.servers {
		err := server.Stop(ctx)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (g *grpcServerResources) setServer(name string, server *grpcserver.Server) {
	g.mu.Lock()
	g.servers[name] = server
	g.mu.Unlock()
}

func (g *grpcServerResources) GetServer(name string) (*grpcserver.Server, error) {
	g.mu.Lock()
	server, ok := g.servers[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_server: server with name %s does not exist", name)
	}
	return server, nil
}

func (g *grpcServerResources) run(ctx context.Context) error {
	errG := errgroup.Group{}
	for name, server := range g.servers {
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_server] starting gRPC server...",
			slog.String("server_name", name),
		)
		errG.Go(func() error {
			return server.Run(ctx)
		})
	}
	return errG.Wait()
}

type grpcGatewayResources struct {
	logger   *slog.Logger
	mu       sync.Mutex
	gateways map[string]*GRPCGatewayObject
}

func (g *grpcGatewayResources) isEmpty() bool {
	g.mu.Lock()
	l := len(g.gateways)
	g.mu.Unlock()
	return l == 0
}

func (g *grpcGatewayResources) close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var errs error
	for _, gw := range g.gateways {
		err := gw.httpServer.Stop(ctx)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (g *grpcGatewayResources) setGateway(name string, gw *GRPCGatewayObject) {
	g.mu.Lock()
	g.gateways[name] = gw
	g.mu.Unlock()
}

func (g *grpcGatewayResources) GetGateway(name string) (*GRPCGatewayObject, error) {
	g.mu.Lock()
	server, ok := g.gateways[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_gateway: gateway with name %s does not exist", name)
	}
	return server, nil
}

func (g *grpcGatewayResources) run(ctx context.Context) error {
	errG := errgroup.Group{}
	for name, gw := range g.gateways {
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_gateway] starting gRPC gateway server...",
			slog.String("grpc_server_name", name),
		)
		errG.Go(func() error {
			return gw.run(ctx)
		})
	}
	return errG.Wait()
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
			slog.String("client_name", c.Name),
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
		resources.Client.setClient(c.Name, conn)
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
		attrs := []slog.Attr{
			slog.String("server_name", server.Name),
			slog.String("server_address", server.Address),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_server] creating gRPC server",
			attrs...,
		)
		s, err := server.create()
		if err != nil {
			g.logger.LogAttrs(
				ctx,
				slog.LevelError,
				"[resources][grpc_server] failed to create gRPC server",
				attrs...,
			)
			return err
		}
		resources.Server.setServer(server.Name, s)
	}
	return nil
}

func (g *GRPCResourcesConfig) createGRPCGateway(ctx context.Context, resources *grpcResources) error {
	for _, server := range g.ServerResources {
		if server.GRPCGateway.Addres == "" {
			continue
		}
		attrs := []slog.Attr{
			slog.String("grpc_server_name", server.Name),
			slog.String("gateway_address", server.GRPCGateway.Addres),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_gateway] creating gRPC gateway server",
			attrs...,
		)
		gw, err := server.GRPCGateway.create()
		if err != nil {
			g.logger.LogAttrs(
				ctx,
				slog.LevelInfo,
				"[resources][grpc_gateway] failed to create gRPC gateway server",
				attrs...,
			)
			return err
		}
		resources.Gateway.setGateway(server.Name, gw)
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

func (g *GRPCGatewayResourceConfig) create() (*GRPCGatewayObject, error) {
	if g.Addres == "" {
		return nil, errors.New("cannot craete grpc gateway with empty address")
	}
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

type GRPCGatewayObject struct {
	mu                sync.Mutex
	servicesHandlerFn []func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error)
	httpServer        *httpserver.Server
}

func (g *GRPCGatewayObject) init(ctx srun.Context) error {
	return g.httpServer.Init(ctx)
}

func (g *GRPCGatewayObject) run(ctx context.Context) error {
	return g.httpServer.Run(ctx)
}

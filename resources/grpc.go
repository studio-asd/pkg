package resources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

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
		logger: logger,
		Client: &grpcClientResources{
			logger:  logger,
			clients: make(map[string]*grpc.ClientConn),
		},
		Server: &grpcServerResources{
			logger:  logger,
			servers: make(map[string]*GRPCServerObject),
		},
		Gateway: &grpcGatewayResources{
			logger:   logger,
			gateways: make(map[string]*GRPCGatewayObject),
		},
	}
}

type grpcResources struct {
	logger  *slog.Logger
	Client  *grpcClientResources
	Server  *grpcServerResources
	Gateway *grpcGatewayResources
}

func (g *grpcResources) init(ctx srun.Context) error {
	if g.Server != nil {
		if err := g.Server.init(ctx); err != nil {
			return err
		}
	}
	if g.Gateway != nil {
		if err := g.Gateway.init(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (g *grpcResources) stop(ctx context.Context) error {
	var errs error
	if len(g.Client.clients) > 0 {
		g.logger.Info("[resources][grpc_client] closing all gRPC clients connections")
		if err := g.Client.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if !g.Server.isEmpty() {
		g.logger.Info("[resources][grpc_server] closing all gRPC servers")
		if err := g.Server.close(ctx); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if !g.Gateway.isEmpty() {
		g.logger.Info("[resources][grpc_gateway] closing all gRPC gateway servers")
		if err := g.Gateway.close(ctx); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
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
	servers map[string]*GRPCServerObject
	address string
}

// GRPCServerObject wraps the grpc server to only expose the function needed to register the services to
// the grpc server.
type GRPCServerObject struct {
	address     string
	server      *grpcserver.Server
	grpcGateway *GRPCGatewayObject
}

// RegisterService registers the grpc services to the grpc server.
func (g *GRPCServerObject) RegisterService(fn func(s grpc.ServiceRegistrar)) {
	g.server.RegisterService(fn)
}

func (g *GRPCServerObject) RegisterGatewayService(fn func(gateway *GRPCGatewayObject) error) error {
	if err := fn(g.grpcGateway); err != nil {
		return err
	}
	return nil
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
		err := server.server.Stop(ctx)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (g *grpcServerResources) setServer(name string, server *GRPCServerObject) {
	g.mu.Lock()
	g.servers[name] = server
	g.mu.Unlock()
}

func (g *grpcServerResources) GetServer(name string) (*GRPCServerObject, error) {
	g.mu.Lock()
	server, ok := g.servers[name]
	g.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc_server: server with name %s does not exist", name)
	}
	return server, nil
}

func (g *grpcServerResources) MustGetServer(name string) *GRPCServerObject {
	server, err := g.GetServer(name)
	if err != nil {
		panic(err)
	}
	return server
}

func (g *grpcServerResources) init(ctx srun.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, server := range g.servers {
		if err := server.server.Init(srun.Context{
			RunnerAppName:    ctx.RunnerAppName,
			RunnerAppVersion: ctx.RunnerAppVersion,
			Ctx:              ctx.Ctx,
			Logger:           slog.Default().With("logger_scope", fmt.Sprintf("%s-grpc-server", server.server.Name())),
			Flags:            ctx.Flags,
			Tracer:           otel.GetTracerProvider().Tracer(server.server.Name()),
			Meter:            otel.GetMeterProvider().Meter(server.server.Name()),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (g *grpcServerResources) run(ctx context.Context) error {
	errG := errgroup.Group{}
	for name, server := range g.servers {
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_server] starting gRPC server...",
			slog.String("grpc_server.name", name),
			slog.String("grpc_server.address", g.address),
		)
		errG.Go(func() error {
			return server.server.Run(ctx)
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
		err := gw.stop(ctx)
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

func (g *grpcGatewayResources) init(ctx srun.Context) error {
	// Init the grpc gateway, because the http server implements srun.RunnerAwareService, some components are
	// being initialized there.
	for name, gw := range g.gateways {
		if err := gw.init(srun.Context{
			RunnerAppName:    ctx.RunnerAppName,
			RunnerAppVersion: ctx.RunnerAppVersion,
			Ctx:              ctx.Ctx,
			Logger:           slog.Default().With("logger_scope", fmt.Sprintf("%s-grpc-gateway-server", name)),
			Flags:            ctx.Flags,
			Tracer:           otel.GetTracerProvider().Tracer(gw.httpServer.Name()),
			Meter:            otel.GetMeterProvider().Meter(gw.httpServer.Name()),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (g *grpcGatewayResources) run(ctx context.Context) error {
	errG := errgroup.Group{}
	for name, gw := range g.gateways {
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_gateway] starting gRPC gateway server...",
			slog.String("grpc_gateway.name", name),
			slog.String("grpc_gateway.address", gw.address),
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
		// Attatch the grpc gateway so its easier for the client to register both the grpc and grpc gateway service.
		gateway, err := resources.Gateway.GetGateway(server.Name)
		if err == nil {
			s.grpcGateway = gateway
		}
		resources.Server.setServer(server.Name, s)
	}
	return nil
}

func (g *GRPCResourcesConfig) createGRPCGateway(ctx context.Context, resources *grpcResources) error {
	for _, server := range g.ServerResources {
		if server.GRPCGateway.Address == "" {
			continue
		}
		attrs := []slog.Attr{
			slog.String("grpc_gateway.name", server.Name),
			slog.String("grpc_gateway.address", server.GRPCGateway.Address),
		}
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_gateway] creating gRPC gateway server",
			attrs...,
		)
		gw, err := server.GRPCGateway.create(server.GRPCGateway.Address)
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
	if g.GRPCGateway.Address != "" {
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

func (g *GRPCServerResourceConfig) create() (*GRPCServerObject, error) {
	server, err := grpcserver.New(grpcserver.Config{
		Address:      g.Address,
		WriteTimeout: time.Duration(g.WriteTimeout),
		ReadTimeout:  time.Duration(g.ReadTimeout),
	})
	if err != nil {
		return nil, err
	}
	return &GRPCServerObject{
		address: g.Address,
		server:  server,
	}, nil
}

type GRPCGatewayResourceConfig struct {
	Address      string `yaml:"address"`
	name         string
	writeTimeout Duration
	readTimeout  Duration
}

func (g *GRPCGatewayResourceConfig) create(serverAddress string) (*GRPCGatewayObject, error) {
	if g.Address == "" {
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
		httpServer:    s,
		address:       g.Address,
		serverAddress: serverAddress,
	}, nil
}

type GRPCGatewayObject struct {
	serverAddress string

	mu         sync.Mutex
	httpServer *httpserver.Server
	address    string

	middlewares     []runtime.Middleware
	errorHandler    runtime.ErrorHandlerFunc
	metadataHandler func(context.Context, *http.Request) metadata.MD
	// While using endpoint is indeed recommended by the grpc-gateway document, but we don't want to use it because of performance
	// and complexities that introduced by it as we mainly use the grpc-gateway to expose our internal APIs to the outside world.
	//
	// By doing so, we are automatically exposing three kind of metrics related to grpc-gateway only:
	// 1. The http server.
	// 2. The grpc client.
	// 3. The grpc server.
	//
	// We don't want this and only want the http server metrics to be exported, thus reducing the complexity of the metrics,
	// the program memory consumption and overall the time needed to GC the memory.
	servicesHandlerFn []func(*runtime.ServeMux) error
}

func (g *GRPCGatewayObject) RegisterMetadataHandler(fn func(context.Context, *http.Request) metadata.MD) {
	g.mu.Lock()
	g.metadataHandler = fn
	g.mu.Unlock()
}

func (g *GRPCGatewayObject) RegisterErrorHandler(fn runtime.ErrorHandlerFunc) {
	g.mu.Lock()
	g.errorHandler = fn
	g.mu.Unlock()
}

func (g *GRPCGatewayObject) RegisterMiddleware(middlewares ...runtime.Middleware) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.middlewares == nil {
		g.middlewares = middlewares
		return
	}
	g.middlewares = append(g.middlewares, middlewares...)
}

func (g *GRPCGatewayObject) RegisterServiceHandler(fn func(mux *runtime.ServeMux) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.servicesHandlerFn = append(g.servicesHandlerFn, fn)
}

func (g *GRPCGatewayObject) init(ctx srun.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.httpServer.Init(ctx)
}

func (g *GRPCGatewayObject) run(ctx context.Context) error {
	var (
		middlewares     []runtime.Middleware
		errorHandler    runtime.ErrorHandlerFunc
		metadataHandler func(context.Context, *http.Request) metadata.MD
	)
	if g.middlewares != nil {
		middlewares = g.middlewares
	}
	if g.errorHandler != nil {
		errorHandler = g.errorHandler
	}
	if g.metadataHandler != nil {
		metadataHandler = g.metadataHandler
	}

	mux := runtime.NewServeMux(
		runtime.WithMiddlewares(middlewares...),
		runtime.WithErrorHandler(errorHandler),
		runtime.WithMetadata(metadataHandler),
		runtime.WithWriteContentLength(),
	)
	for _, fn := range g.servicesHandlerFn {
		if err := fn(mux); err != nil {
			return err
		}
	}
	g.httpServer.SetHandler(mux)
	return g.httpServer.Run(ctx)
}

func (g *GRPCGatewayObject) stop(ctx context.Context) error {
	return g.httpServer.Stop(ctx)
}

package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"

	"github.com/studio-asd/pkg/srun"
)

var _ srun.ServiceRunnerAware = (*Server)(nil)

type Server struct {
	config             Config
	registerServiceFns []func(s grpc.ServiceRegistrar)

	stateHelper *srun.StateHelper
	stopMu      sync.Mutex
	// All variables below is assigned after the object is created(in Init, etc).
	logger   *slog.Logger
	listener net.Listener
	server   *grpc.Server
}

func New(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Server{
		config: config,
	}, nil
}

func (s *Server) Name() string {
	return "grpc-server"
}

func (s *Server) RegisterService(fn func(s grpc.ServiceRegistrar)) {
	s.registerServiceFns = append(s.registerServiceFns, fn)
}

func (s *Server) Init(ctx srun.Context) error {
	s.stateHelper = ctx.NewStateHelper("grpc-server")
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.listener = listener
	s.logger = ctx.Logger.With(slog.String("logger.scope", "grpc-server"))

	otelServerOption := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{
			MeterProvider: otel.GetMeterProvider(),
		},
	})
	s.server = grpc.NewServer(
		otelServerOption,
		// WaitForHandlers is an experimental feature in gRPC to wait for all handlers to return first.
		// By default graceful shutdown only waits for all connections to close and not handlers.
		grpc.WaitForHandlers(true),
		// StatsHandler is used to export prometheus metrics for grpc handlers.
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithMetricAttributes(s.config.Meter.DefaultAttributes...),
			otelgrpc.WithSpanAttributes(s.config.Trace.DefaultAttributes...),
			otelgrpc.WithPropagators(
				propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{}),
			),
		)),
	)
	if s.config.Trace.Tracer == nil {
		s.config.Trace.Tracer = ctx.Tracer
	}
	if s.config.Meter.Meter == nil {
		s.config.Meter.Meter = ctx.Meter
	}
	return nil
}

func (s *Server) Ready(ctx context.Context) error {
	if s.stateHelper.IsRunning() {
		return nil
	}
	return errors.New("service is in stopped state")
}

func (s *Server) Run(ctx context.Context) error {
	// Register all the services into the server.
	for _, fn := range s.registerServiceFns {
		fn(s.server)
	}

	errC := make(chan error, 1)
	go func() {
		errC <- s.server.Serve(s.listener)
	}()
	// Publish ready state after a while to give some time for goroutine is scheduled
	// and grpc server is running.
	time.AfterFunc(time.Millisecond*300, func() {
		s.stateHelper.SetRunning()
	})
	// Because srun will always wait for the run function to return, we can set the state
	// back to stopped here.
	defer s.stateHelper.SetStopped()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errC:
		return err
	}
}

func (s *Server) Stop(ctx context.Context) error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.server.GracefulStop()
	return nil
}

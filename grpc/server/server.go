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
	"go.opentelemetry.io/otel/attribute"
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

func New() {
}

func (s *Server) Name() string {
	return "grpc-server"
}

func (s *Server) RegisterService(fn func(s grpc.ServiceRegistrar)) {
	s.registerServiceFns = append(s.registerServiceFns, fn)
}

func (s *Server) Init(ctx srun.Context) error {
	s.stateHelper = ctx.StateHelper
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.listener = listener
	s.logger = ctx.Logger

	metricAttrs := []attribute.KeyValue{
		attribute.String("service_name", ctx.RunnerAppName),
		attribute.String("service_version", ctx.RunnerAppVersion),
	}
	if s.config.Meter.DefaultAttributes != nil {
		metricAttrs = append(metricAttrs, s.config.Meter.DefaultAttributes...)
	}

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
			otelgrpc.WithMetricAttributes(metricAttrs...),
		)),
	)
	if s.config.Trace.Tracer == nil {
		s.config.Trace.Tracer = ctx.Tracer
	}
	if s.config.Meter.Meter == nil {
		s.config.Meter.Meter = ctx.Meter
	}
	// Register all the services into the server.
	for _, fn := range s.registerServiceFns {
		fn(s.server)
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

package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/studio-asd/pkg/srun"
)

var _ srun.ServiceRunnerAware = (*Server)(nil)

const (
	defaultReadTimeout       = time.Minute
	defaultReadHeaderTimeout = time.Second * 20
	defaultWriteTimeout      = time.Second * 30
)

type Config struct {
	Name              string
	Address           string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return errors.New("http: address cannot be empty")
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	return nil
}

type Server struct {
	config Config
	server *http.Server

	logger      *slog.Logger
	stateHelper *srun.StateHelper
}

func New(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Server{
		config: config,
		server: &http.Server{
			ReadTimeout:       config.ReadTimeout,
			ReadHeaderTimeout: config.ReadHeaderTimeout,
			WriteTimeout:      config.WriteTimeout,
		},
	}, nil
}

func (s *Server) SetHandler(handler http.Handler) {
	s.server.Handler = handler
}

func (s *Server) Name() string {
	if s.config.Name != "" {
		return strings.Join([]string{s.config.Name, "http-server"}, "-")
	}
	return "http-server"
}

func (s *Server) Init(ctx srun.Context) error {
	s.stateHelper = ctx.NewStateHelper("http-server")
	s.logger = ctx.Logger
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	errC := make(chan error, 1)

	time.AfterFunc(time.Millisecond*300, func() {
		s.stateHelper.SetRunning()
	})

	s.logger.LogAttrs(
		ctx,
		slog.LevelInfo,
		"[http_server] starting HTTP server",
		slog.String("address", s.config.Address),
	)
	go func() {
		errC <- s.server.Serve(listener)
	}()
	defer s.stateHelper.SetStopped()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errC:
		return err
	}
}

func (s *Server) Ready(ctx context.Context) error {
	if s.stateHelper.IsRunning() {
		return nil
	}
	return errors.New("http_server: the server is stopped")
}

func (s *Server) Stop(ctx context.Context) error {
	if s.stateHelper.IsStopped() {
		return nil
	}
	s.logger.LogAttrs(
		ctx,
		slog.LevelInfo,
		"[http_server] stopping HTTP server",
		slog.String("address", s.config.Address),
	)
	return s.server.Shutdown(ctx)
}

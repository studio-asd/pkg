package srun

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const defaultAdminHTTPServerAddress = ":8778"

var _ ServiceRunnerAware = (*adminHTTPServer)(nil)

// adminHTTPServerDefaultConfig is the admin server default configuration to ensure the admin
// server to be able to serve endpoint without any additional configuration.
var adminHTTPServerDefaultConfig = AdminHTTPServerConfig{
	WriteTimeout:      time.Minute,
	ReadTimeout:       time.Minute,
	ReadHeaderTimeout: time.Minute,
	IdleTimeout:       time.Second * 30,
}

type AdminConfig struct {
	// Disable disables administration http server. We don't recommend this to be turned off in non-testing mode.
	Disable bool
	AdminServerConfig
}

type AdminServerConfig struct {
	// prometheushandlerDisabled is a flag to decide whether we need to serve /metrics endpoint
	// or not. This configuration is affected by otel configuration.
	prometheusHandlerDisabled bool

	Address          string
	HTTPServerConfig AdminHTTPServerConfig
	ReadinessFunc    func() error
	HealthcheckFunc  func() error
}

type AdminHTTPServerConfig struct {
	WriteTimeout      time.Duration
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
}

func (c *AdminServerConfig) validate() error {
	if c.Address == "" {
		c.Address = defaultAdminHTTPServerAddress
	}
	if reflect.ValueOf(c.HTTPServerConfig).IsZero() {
		c.HTTPServerConfig = adminHTTPServerDefaultConfig
	}
	if c.ReadinessFunc == nil {
		c.ReadinessFunc = func() error {
			return nil
		}
	}
	if c.HealthcheckFunc == nil {
		c.HealthcheckFunc = func() error {
			return nil
		}
	}
	return nil
}

type adminHTTPServer struct {
	listener net.Listener
	server   *http.Server
	config   AdminServerConfig
	readyC   chan struct{}
}

func newAdminServer(config AdminServerConfig) (*adminHTTPServer, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &adminHTTPServer{
		server: &http.Server{},
		config: config,
		readyC: make(chan struct{}, 1),
	}, nil
}

func (a *adminHTTPServer) Name() string {
	return "srun-http-admin-server"
}

func (a *adminHTTPServer) Init(Context) error {
	listener, err := net.Listen("tcp", a.config.Address)
	if err != nil {
		return err
	}
	a.listener = listener
	return nil
}

func (a *adminHTTPServer) Run(ctx context.Context) error {
	httpServer := &http.Server{
		Handler: handler(a.config),
	}
	a.server = httpServer
	time.AfterFunc(time.Millisecond*500, func() {
		a.readyC <- struct{}{}
	})
	return httpServer.Serve(a.listener)
}

func (a *adminHTTPServer) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.readyC:
	}
	return nil
}

func (a *adminHTTPServer) Stop(ctx context.Context) error {
	if a.server != nil {
		err := a.server.Shutdown(ctx)
		if err != nil {
			return err
		}
	}
	// We close the listener here for just in case we somehow failed in the init state and server
	// is not running yet.
	if err := a.listener.Close(); err != nil {
		return err
	}
	return nil
}

func (a *adminHTTPServer) SetReadinessFunc(fn func() error) {
	a.config.ReadinessFunc = fn
}

func (a *adminHTTPServer) SetHealthCheckFunc(fn func() error) {
	a.config.HealthcheckFunc = fn
}

func handler(config AdminServerConfig) *http.ServeMux {
	mux := http.NewServeMux()
	// Prometheus metrics endpoint.
	if !config.prometheusHandlerDisabled {
		mux.Handle("GET /metrics", promhttp.Handler())
	}
	// Pprof endpoints.
	mux.HandleFunc("GET /debug/pprof", func(w http.ResponseWriter, r *http.Request) {
		pprof.Index(w, r)
	})
	mux.HandleFunc("GET /debug/cmdline", func(w http.ResponseWriter, r *http.Request) {
		pprof.Cmdline(w, r)
	})
	mux.HandleFunc("GET /debug/profile", func(w http.ResponseWriter, r *http.Request) {
		pprof.Profile(w, r)
	})
	mux.HandleFunc("GET /debug/symbol", func(w http.ResponseWriter, r *http.Request) {
		pprof.Symbol(w, r)
	})
	mux.HandleFunc("GET /debug/trace", func(w http.ResponseWriter, r *http.Request) {
		pprof.Trace(w, r)
	})
	mux.HandleFunc("/debug/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		pprof.Handler(name).ServeHTTP(w, r)
	})
	return mux
}

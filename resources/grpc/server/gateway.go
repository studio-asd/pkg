package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/studio-asd/pkg/instrumentation"
	"github.com/studio-asd/pkg/resources"
	"github.com/studio-asd/pkg/srun"

	httpserver "github.com/studio-asd/pkg/http/server"
)

func (g *GRPCServerLib) createGRPCGateway(ctx context.Context, config resources.GRPCGatewayResourceConfig) (*GRPCGateway, error) {
	if config.Address == "" {
		return nil, errors.New("cannot craete grpc gateway with empty address")
	}
	attrs := []slog.Attr{
		slog.String("grpc_gateway.name", config.Name),
		slog.String("grpc_gateway.address", config.Address),
	}
	g.logger.LogAttrs(
		ctx,
		slog.LevelInfo,
		"[resources][grpc_gateway] creating gRPC gateway server",
		attrs...,
	)
	s, err := httpserver.New(httpserver.Config{
		Address:      config.Address,
		Name:         fmt.Sprintf("%s-grpc-gateway", config.Name),
		WriteTimeout: time.Duration(config.WriteTimeout),
		ReadTimeout:  time.Duration(config.ReadTimeout),
	})
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		g.logger.LogAttrs(
			ctx,
			slog.LevelInfo,
			"[resources][grpc_gateway] failed to create gRPC gateway server",
			attrs...,
		)
		return nil, err
	}
	// Initialize the http server as some of its components depends on the srun.Context.
	if err := s.Init(srun.Context{
		Ctx:    ctx,
		Logger: slog.Default().With("logger_scope", fmt.Sprintf("%s-grpc-gateway-server", config.Name)),
		Tracer: otel.GetTracerProvider().Tracer(s.Name()),
		Meter:  otel.GetMeterProvider().Meter(s.Name()),
	}); err != nil {
		return nil, err
	}
	return &GRPCGateway{
		httpServer:    s,
		address:       config.Address,
		serverAddress: config.ServerAddress,
	}, nil
}

type GRPCGateway struct {
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

func (g *GRPCGateway) RegisterMetadataHandler(fn func(context.Context, *http.Request) metadata.MD) {
	g.mu.Lock()
	g.metadataHandler = fn
	g.mu.Unlock()
}

func (g *GRPCGateway) RegisterErrorHandler(fn runtime.ErrorHandlerFunc) {
	g.mu.Lock()
	g.errorHandler = fn
	g.mu.Unlock()
}

func (g *GRPCGateway) RegisterMiddleware(middlewares ...runtime.Middleware) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.middlewares == nil {
		g.middlewares = middlewares
		return
	}
	g.middlewares = append(g.middlewares, middlewares...)
}

func (g *GRPCGateway) RegisterServiceHandler(fn func(mux *runtime.ServeMux) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.servicesHandlerFn = append(g.servicesHandlerFn, fn)
}

func (g *GRPCGateway) init(ctx srun.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.httpServer.Init(ctx)
}

func (g *GRPCGateway) run(ctx context.Context) error {
	var (
		middlewares = []runtime.Middleware{
			// Always set the most bottom middleware to extract information for Open Telemetry. We need to retrieve the information from inside
			// the grpc-gateway middleware because this information is non existent pre runtime.Mux.
			func(hf runtime.HandlerFunc) runtime.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
					pattern, ok := runtime.HTTPPattern(r.Context())
					if !ok {
						return
					}
					d, ok := r.Context().Value(otelHTTPInfoDelegatorKey).(*otelHTTPInfoDelegator)
					if !ok {
						return
					}
					d.HTTPPattern = pattern.String()
					hf(w, r, pathParams)
				}
			},
		}
		errorHandler    runtime.ErrorHandlerFunc
		metadataHandler func(context.Context, *http.Request) metadata.MD
	)
	if g.middlewares != nil {
		middlewares = append(middlewares, g.middlewares...)
	}
	if g.errorHandler != nil {
		errorHandler = g.errorHandler
	}
	if g.metadataHandler != nil {
		metadataHandler = g.metadataHandler
	}

	// If the error handler is nil, then we will set the default error handler.
	if errorHandler == nil {
		errorHandler = runtime.DefaultHTTPErrorHandler
	}

	mux := runtime.NewServeMux(
		runtime.WithMiddlewares(middlewares...),
		runtime.WithErrorHandler(errorHandler),
		runtime.WithWriteContentLength(),
		runtime.WithMetadata(metadataHandler),
	)
	for _, fn := range g.servicesHandlerFn {
		if err := fn(mux); err != nil {
			return err
		}
	}

	// Wraps the runtime.Mux handler with the http.Handler that expose Open Telemetry metrics.
	// Create a OtelHandler and wrap everything around it to monitor the http requests.
	otelHandler := otelhttp.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
		}),
		"grpc.gateway.handler",
		otelhttp.WithMetricAttributesFn(func(r *http.Request) []attribute.KeyValue {
			d, ok := r.Context().Value(otelHTTPInfoDelegatorKey).(*otelHTTPInfoDelegator)
			if !ok {
				return nil
			}
			attrs := []attribute.KeyValue{
				attribute.String("http.request.pattern", d.HTTPPattern),
				attribute.String("http.request.method", r.Method),
			}
			attrs = append(
				attrs,
				instrumentation.BaggageFromContext(r.Context()).ToOpenTelemetryAttributesForMetrics()...,
			)
			return attrs
		}),
		otelhttp.WithTracerProvider(otel.GetTracerProvider()),
		otelhttp.WithMeterProvider(otel.GetMeterProvider()),
		otelhttp.WithPropagators(
			propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{}),
		),
	)
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Put the OtelHTTPInfoDelegator to the context so the Open Telemetry handler can access it.
		r = r.WithContext(context.WithValue(r.Context(), otelHTTPInfoDelegatorKey, &otelHTTPInfoDelegator{}))
		otelHandler.ServeHTTP(w, r)
	})
	g.httpServer.SetHandler(wrappedHandler)
	return g.httpServer.Run(ctx)
}

func (g *GRPCGateway) stop(ctx context.Context) error {
	return g.httpServer.Stop(ctx)
}

type contextKey struct{}

// otelHTTPInfoDelegatorKey is the key used to store the OtelHTTPInfoDelegator object in the http request context.
var otelHTTPInfoDelegatorKey contextKey = struct{}{}

// otelHTTPInfoDelegator is an object that delegated to the http request context to store the information
// for the Open Telemetry instrumentation. This object is needed because we are wrapping the runtime.Mux
// handler with the Open Telemetry handler, so the rich information cannot be received before the url
// is being routed to the handler. In order to keep the rich information, we need to store it in the context,
// pass it to the open telemetry handler, and somehow retrieve the object back with the injected information.
// While this is complex and eats more memory, a workaround currently is not available so we need to use this.
type otelHTTPInfoDelegator struct {
	HTTPPattern string
}

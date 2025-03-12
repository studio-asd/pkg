package client

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats/opentelemetry"
)

// Please check the official documentation for grpc service config.
// https://github.com/grpc/grpc/blob/master/doc/service_config.md
const grpcServiceConfig = `{
	"loadBalancingPolicy": "round_robin",
}
`

// New creates a new gRPC client that use round_robin loadBalancingPolicy by default.
func New(ctx context.Context, c Config) (*grpc.ClientConn, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	otelDial := opentelemetry.DialOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{
			MeterProvider: otel.GetMeterProvider(),
		},
	})
	conn, err := grpc.NewClient(
		c.Address,
		otelDial,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithMetricAttributes(c.DefaultAttributes...),
			otelgrpc.WithSpanAttributes(c.DefaultAttributes...),
		)),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

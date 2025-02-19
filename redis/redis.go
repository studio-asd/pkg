// The redis package supports redis version >= 6.0

package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	rd "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
)

type Config struct {
	ClientName     string
	Address        string
	Username       string
	Password       string
	DialTimeout    time.Duration
	MaxActiveConns int
	MaxIdleConns   int
	MaxRetries     int
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	PoolTimeout    time.Duration
}

func (c Config) buildRedisOptions() (*rd.Options, error) {
	// By default the context timeout option is enabled as we want to respect the context timeout
	// from the parent request.
	options := &rd.Options{
		Network:               "tcp",
		ContextTimeoutEnabled: true,
	}
	// ClientName.
	if c.ClientName == "" {
		return nil, errors.New("client name cannot be empty")
	}
	options.ClientName = c.ClientName
	// Address.
	if c.Address == "" {
		return nil, errors.New("address cannot be empty")
	}
	options.Addr = c.Address
	// Username & Password.
	if c.Username != "" && c.Password != "" {
		options.Username = c.Username
		options.Password = c.Password
	}
	// Dial timeout.
	if c.DialTimeout > 0 {
		options.DialTimeout = c.DialTimeout
	}
	// MaxActiveConns.
	if c.MaxActiveConns > 0 {
		options.MaxActiveConns = c.MaxActiveConns
	}
	// MaxIdleConns.
	if c.MaxIdleConns > 0 {
		options.MaxIdleConns = c.MaxIdleConns
	}
	// ReadTimeout.
	if c.ReadTimeout > 0 {
		options.ReadTimeout = c.ReadTimeout
	}
	// WriteTimeout.
	if c.WriteTimeout > 0 {
		options.WriteTimeout = c.WriteTimeout
	}
	// PoolTimeout.
	if c.PoolTimeout > 0 {
		options.PoolTimeout = c.PoolTimeout
	}
	// MaxRetries.
	if c.MaxRetries > 0 {
		options.MaxRetries = c.MaxRetries
	}
	return options, nil
}

func New(config Config) (*rd.Client, error) {
	client := rd.NewClient(&rd.Options{
		// Ping the redis when the connection is established.
		OnConnect: func(ctx context.Context, cn *rd.Conn) error {
			status := cn.Ping(ctx)
			if err := status.Err(); err != nil {
				return err
			}
			return nil
		},
	})

	defaultAttrs := []attribute.KeyValue{
		attribute.String("operation_name", "redis_command"),
	}

	if err := redisotel.InstrumentTracing(
		client,
		redisotel.WithAttributes(
			defaultAttrs...,
		),
	); err != nil {
		return nil, err
	}
	if err := redisotel.InstrumentMetrics(
		client,
		redisotel.WithAttributes(
			defaultAttrs...,
		),
	); err != nil {
		return nil, err
	}
	return client, nil
}

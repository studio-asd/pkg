package client

import (
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
)

type Config struct {
	Timeout time.Duration
	Jar     http.CookieJar
}

type Client struct {
	client *http.Client
}

func New(config Config) *Client {
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(
			http.DefaultTransport,
			otelhttp.WithPropagators(
				propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{}),
			),
		),
	}
	if config.Timeout > 0 {
		httpClient.Timeout = config.Timeout
	}
	if config.Jar != nil {
		httpClient.Jar = config.Jar
	}
	return &Client{
		client: httpClient,
	}
}

func (c *Client) Do(req *Request) (*http.Response, error) {
	httpReq, err := req.Compile()
	if err != nil {
		return nil, err
	}
	return c.client.Do(httpReq)
}

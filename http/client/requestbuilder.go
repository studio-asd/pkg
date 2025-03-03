package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"

	"github.com/studio-asd/pkg/instrumentation"
)

// Request store the information to create a http.Request via compile method.
//
// Request is not concurrently safe and meant to be used in a single goroutine.
type Request struct {
	err error

	method   string
	url      string
	queryKvs []string
	header   http.Header

	body io.Reader
	ctx  context.Context

	// For monitoring purpose.
	//lint:ignore U1000 we need this for future usage.
	name string
}

func NewRequestBuilder(ctx context.Context) *Request {
	r := Request{
		ctx: ctx,
	}

	// Inject the instrumentation baggage to the http header for context propagation.
	// header := instrumentation.BaggageFromContext(ctx).ToHTTPHeader()
	// if header != nil {
	// Flag the header that this header is injected with instrumentation. This header is
	// special to accommodate the header injection in the pkg/http/client.
	// header.Add(httppkg.HeaderInstrumentationInject, "1")
	// r.header = header
	// }
	return &r
}

func (r *Request) Header(header http.Header) *Request {
	if header == nil {
		return r
	}
	if r.header == nil {
		r.header = http.Header{}
	}
	for k := range header {
		for _, v := range header[k] {
			r.header.Set(k, v)
		}
	}
	return r
}

// Method function set the request method
func (r *Request) Method(method string) *Request {
	r.method = method
	return r
}

// URL function set the request url
func (r *Request) URL(url string) *Request {
	if url == "" {
		r.err = errors.New("cannot set empty request url")
	}
	r.url = url
	return r
}

// Name for monitoring, naming the request
func (r *Request) Name(v string) *Request {
	if v == "" {
		return r
	}
	r.name = v
	return r
}

// Get function for building get request
func (r *Request) Get(url string) *Request {
	if r.err != nil {
		return r
	}
	r.method = http.MethodGet
	r.URL(url)
	return r
}

func (r *Request) Query(kv ...string) *Request {
	if r.err != nil {
		return r
	}

	if len(kv) == 0 {
		return r
	}

	// Avoid to create a query that cause panic return the request if kv length is not even.
	if len(kv)%2 != 0 {
		r.err = errors.New(`http-request: query parameters is not even, parameters must be passed in key, value form. For example Query("key", "value")`)
		return r
	}
	r.queryKvs = kv
	return r
}

// Post function for building post request
func (r *Request) Post(url string) *Request {
	if r.err != nil {
		return r
	}
	r.method = http.MethodPost
	r.URL(url)
	return r
}

// PostForm set a url values for a post form body in a request.
func (r *Request) PostForm(kv ...string) *Request {
	if r.method != "" && r.method != http.MethodPost {
		r.err = fmt.Errorf("cannot use post form for %s request method", r.method)
		return r
	}

	// Avoid to create a query that cause panic, return the request if kv length is not even.
	if len(kv)%2 != 0 {
		r.err = errors.New(`http-request: post-form parameters is not even, parameters must be passed in key, value form. For example PostForm("key", "value")`)
		return r
	}

	data := url.Values{}
	for idx := range kv {
		if idx > 0 {
			idx++
			if idx == len(kv)-1 {
				break
			}
		}
		data.Add(kv[idx], kv[idx+1])
	}
	// Expected to create a body, and not append the post form to save allocations.
	encodedData := data.Encode()
	r.body = strings.NewReader(encodedData)

	// Check the header because we need to set the Content-Type header value to
	// application/x-www-form-urlencoded.
	if r.header == nil {
		r.header = http.Header{}
	}
	r.header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

// Patch function for building patch request
func (r *Request) Patch(url string) *Request {
	if r.err != nil {
		return r
	}
	r.method = http.MethodPatch
	r.URL(url)
	return r
}

// Put function for building put request
func (r *Request) Put(url string) *Request {
	if r.err != nil {
		return r
	}
	r.method = http.MethodPut
	r.URL(url)
	return r
}

// Delete function for building delete request
func (r *Request) Delete(url string) *Request {
	if r.err != nil {
		return r
	}
	r.method = http.MethodDelete
	r.URL(url)
	return r
}

func (r *Request) Body(body io.Reader) *Request {
	r.body = body
	return r
}

// BodyJSON indicate that request body is a json data.
func (r *Request) BodyJSON(body interface{}) *Request {
	if r.err != nil {
		return r
	}

	var buff *bytes.Buffer

	switch t := body.(type) {
	case []byte:
		buff = bytes.NewBuffer(t)
	case string:
		buff = bytes.NewBufferString(t)
	default:
		out, err := json.Marshal(body)
		if err != nil {
			r.err = err
			return r
		}
		buff = bytes.NewBuffer(out)
	}
	r.body = buff

	if r.header == nil {
		r.header = http.Header{}
	}
	r.header.Set("Content-Type", "application/json")
	return r
}

// Compile the entire request struct into http.Request.
func (r *Request) Compile() (*http.Request, error) {
	// Check whether the previous steps produce any error.
	if r.err != nil {
		return nil, r.err
	}

	u, err := url.Parse(r.url)
	if err != nil {
		return nil, err
	}
	addURLQuery(u, r.queryKvs...)

	// Add opentelemetry instrumentation to the metrics collected by the client.
	bg := instrumentation.BaggageFromContext(r.ctx)
	labeler := &otelhttp.Labeler{}
	labeler.Add(bg.ToOpenTelemetryAttributesForMetrics()...)
	if r.name != "" {
		labeler.Add(attribute.String("http_request_name", r.name))
	}
	newCtx := otelhttp.ContextWithLabeler(r.ctx, labeler)

	req, err := http.NewRequestWithContext(newCtx, r.method, u.String(), r.body)
	if err != nil {
		return nil, err
	}

	// Inject the instrumentation baggage to the http header for context propagation.
	header := bg.ToHTTPHeader()
	if header != nil {
		// Flag the header that this header is injected with instrumentation. This header is
		// special to accommodate the header injection in the pkg/http/client.
		r.header = header
	}

	if r.header != nil {
		req.Header = r.header
	} else {
		req.Header = http.Header{}
	}
	return req, nil
}

func addURLQuery(u *url.URL, kvs ...string) {
	if len(kvs) == 0 {
		return
	}

	q := u.Query()

	if len(kvs) == 2 {
		q.Add(kvs[0], kvs[1])
		u.RawQuery = q.Encode()
		return
	}

	for idx := range kvs {
		if idx > 0 {
			idx += idx
			if idx == len(kvs) {
				break
			}
		}
		q.Add(kvs[idx], kvs[idx+1])
	}
	u.RawQuery = q.Encode()
}

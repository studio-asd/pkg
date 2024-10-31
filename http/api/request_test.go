package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/albertwidi/pkg/http/requestbuilder"
)

func TestSetPathParamsValue(t *testing.T) {
	t.Parallel()

	type PathParam1 struct {
		Key string `path:"key"`
	}

	type PathParam2 struct {
		StringKey  string  `path:"string"`
		IntKey     int     `path:"int"`
		Int64Key   int64   `path:"int64"`
		Float32Key float32 `path:"float32"`
		Float64Key float64 `path:"float64"`
		BoolKey    bool    `path:"bool"`
	}

	tests := []struct {
		name   string
		params func(*testing.T) *http.Request
		str    any
		expect any
	}{
		{
			name: "simple path params",
			params: func(t *testing.T) *http.Request {
				t.Helper()
				r, err := http.NewRequest(http.MethodGet, "localhost", nil)
				if err != nil {
					t.Fatal(err)
				}
				r.SetPathValue("key", "value")
				return r
			},
			str:    &PathParam1{},
			expect: &PathParam1{Key: "value"},
		},
		{
			name: "complete path params",
			params: func(t *testing.T) *http.Request {
				t.Helper()
				r, err := http.NewRequest(http.MethodGet, "localhost", nil)
				if err != nil {
					t.Fatal(err)
				}
				r.SetPathValue("string", "string_key")
				r.SetPathValue("int", "10")
				r.SetPathValue("int64", "10000")
				r.SetPathValue("float32", "10.10")
				r.SetPathValue("float64", "1000.10")
				r.SetPathValue("bool", "true")
				return r
			},
			str: &PathParam2{},
			expect: &PathParam2{
				StringKey:  "string_key",
				IntKey:     10,
				Int64Key:   10000,
				Float32Key: 10.10,
				Float64Key: 1000.10,
				BoolKey:    true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Because test.str is 'any' we can't use &test.str as it will turn into a pointer
			// to the interface. Thus we can't edit the value of pointer to interface. The
			// interface itself need to hold a pointer of the struct so we can edit the
			// path params value.
			if err := readPathParamsValue(test.params(t), test.str); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(test.expect, test.str); diff != "" {
				t.Fatalf("(-want/+got)\n%s", diff)
			}
		})
	}
}

func TestReadURLEncodedParams(t *testing.T) {
	t.Parallel()

	type testGetRequest struct {
		RequestTypeQueryString
		DummySchemaExample
		DummyValidate
		Key1 string   `schema:"key1"`
		Key2 []string `schema:"key2"`
	}

	type testPostFormRequest struct {
		RequestTypePostForm
		DummySchemaExample
		DummyValidate
		Key1 string `schema:"key1"`
		Key2 string `schema:"key2"`
	}

	tests := []struct {
		name             string
		request          func(t *testing.T) *http.Request
		str              Request
		expect           any
		expectHTTPMethod string
	}{
		{
			name: "http get query string",
			request: func(t *testing.T) *http.Request {
				t.Helper()

				newURL, err := url.Parse("http://localhost?key1=value&key2=value&key2=key")
				if err != nil {
					t.Fatal(err)
				}
				req, err := http.NewRequest(http.MethodGet, newURL.String(), nil)
				if err != nil {
					t.Fatal(err)
				}
				return req
			},
			str: &testGetRequest{},
			expect: &testGetRequest{
				Key1: "value",
				Key2: []string{"value", "key"},
			},
			expectHTTPMethod: http.MethodGet,
		},
		{
			name: "http post form request",
			request: func(t *testing.T) *http.Request {
				t.Helper()

				req, err := requestbuilder.New(context.Background()).
					Post("http://localhost").
					PostForm("key1", "value1", "key2", "value2").
					Compile()
				if err != nil {
					t.Fatal(err)
				}
				// Parse the post-form immediately because we want to read it in our test.
				// Without this, then the r.PostForm will be left unparsed.
				if err := req.ParseForm(); err != nil {
					t.Fatal(err)
				}
				return req
			},
			str: &testPostFormRequest{},
			expect: &testPostFormRequest{
				Key1: "value1",
				Key2: "value2",
			},
			expectHTTPMethod: http.MethodPost,
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := tt.request(t)
			if req.Method != tt.expectHTTPMethod {
				t.Fatalf("expecting http method %s but got %s", tt.expectHTTPMethod, req.Method)
			}

			str := tt.str
			if err := readURLEncodedParameters(req, tt.str, tt.str.RequestTypes()[0]); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tt.expect, str); diff != "" {
				t.Fatalf("invalid url encoded request struct: (-want/+got)\n%s", diff)
			}
		})
	}
}

type TestRequestMultipleTypes struct {
	RequestTypePathParamsAndJSON
	Param1 string `path:"param_1"`
	Param2 string `path:"param_2"`
	Body   string `json:"body"`
}

func (t *TestRequestMultipleTypes) Validate() error {
	return nil
}

func (t *TestRequestMultipleTypes) SchemaExample() RequestResponse {
	return &TestAPIAutoErrorRequest{}
}

func TestMultipleRequestTypes(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", ":")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	httpClient := &http.Client{}

	m := NewMux()
	// In this test we need to handle the error manually because we bypass the 'api/mux' and use 'pkg/mux' directly.
	m.m.Put("/v1/{param_1}/{param_2}", func(w http.ResponseWriter, r *http.Request) error {
		req := &TestRequestMultipleTypes{}
		err := ReadRequest(r, req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return err
		}
		if req.Param1 != "param_1" {
			err := errors.New("expecting param_1 value for param_1")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return err
		}
		if req.Param2 != "param_2" {
			err := errors.New("expecting param_2 value for param_2")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return err
		}
		if req.Body != "hello" {
			err := errors.New("expecting hello for requst body")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return err
		}
		return nil
	})

	var errC chan error
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	server := &http.Server{
		Handler: m,
	}
	go func() {
		errC <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		break
	case err := <-errC:
		t.Fatal(err)
	}

	baseURL := fmt.Sprintf("http://%s/v1/param_1/param_2", listener.Addr().String())
	req, err := requestbuilder.New(context.Background()).
		Method(http.MethodPut).
		URL(baseURL).
		BodyJSON(`{"body": "hello"}`).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		out, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("expecting ok status but got %d. With body %s", resp.StatusCode, string(out))
	}
}

// DummySchemaExample is used to test a struct type request without implementing the schema example function.
type DummySchemaExample struct{}

func (d DummySchemaExample) SchemaExample() RequestResponse {
	return DummyRequestResponse{}
}

type DummyRequestResponse struct{}

func (d DummyRequestResponse) internalRequestResponse() {}

type DummyValidate struct{}

func (d DummyValidate) Validate() error {
	return nil
}

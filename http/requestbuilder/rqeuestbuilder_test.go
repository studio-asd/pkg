package requestbuilder

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateRequest(t *testing.T) {
	tests := []struct {
		name        string
		constructor func(name, url string, req *Request)
		handler     func(t *testing.T) http.Handler
	}{
		{
			name: "simple post http request",
			constructor: func(name, url string, req *Request) {
				req.Name(name).
					Get(url).
					Header(http.Header{
						"Content-Type": []string{"application/json"},
					})
			},
			handler: func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet {
						t.Fatalf("expecting method GET but got %s", r.Method)
					}

					if r.Header.Get("Content-Type") != "application/json" {
						t.Fatalf("expecting content type application/json but got %s", r.Header.Get("Content-Type"))
					}

					w.WriteHeader(http.StatusOK)
					w.Write([]byte("OK"))
				})
			},
		},
		{
			"post form http request",
			func(name, url string, req *Request) {
				req.Name(name).
					Post(url).
					BodyJSON(map[string]string{
						"key1": "value1",
						"key2": "value2",
					})
			},
			func(t *testing.T) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost {
						t.Fatalf("expecting method POST but got %s", r.Method)
					}

					if r.Header.Get("Content-Type") != "application/json" {
						t.Fatalf("invalid content-type, got %s", r.Header.Get("Content-Type"))
					}

					out, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}

					expectJSON := `{"key1":"value1","key2":"value2"}`
					if string(out) != expectJSON {
						t.Fatalf("expecting %s but got %s", expectJSON, string(out))
					}

					w.WriteHeader(http.StatusOK)
					w.Write([]byte("OK"))
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := test.handler(t)
			server := httptest.NewServer(handler)
			defer server.Close()

			req := New(context.Background())
			test.constructor(test.name, server.URL, req)

			httpreq, err := req.Compile()
			if err != nil {
				t.Fatal(err)
			}

			client := server.Client()

			resp, err := client.Do(httpreq)
			if err != nil {
				t.Fatal(err)
			}

			if resp != nil {
				defer resp.Body.Close()
			}
		})
	}
}

package mux

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method       string
		pattern      string
		builder      func(m *Mux)
		handler      HandlerFunc
		responseCode int
	}{
		{
			method:  http.MethodGet,
			pattern: "/",
			builder: func(m *Mux) {
				m.Get("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodPost,
			pattern: "/",
			builder: func(m *Mux) {
				m.Post("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodPut,
			pattern: "/",
			builder: func(m *Mux) {
				m.Put("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodPatch,
			pattern: "/",
			builder: func(m *Mux) {
				m.Patch("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodDelete,
			pattern: "/",
			builder: func(m *Mux) {
				m.Delete("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodOptions,
			pattern: "/",
			builder: func(m *Mux) {
				m.Options("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
		{
			method:  http.MethodHead,
			pattern: "/",
			builder: func(m *Mux) {
				m.Head("/", buildDummy200Handler())
			},
			responseCode: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			m := New()
			test.builder(m)

			server := httptest.NewServer(m)
			client := server.Client()

			target, err := url.JoinPath(server.URL, test.pattern)
			if err != nil {
				t.Fatal(err)
			}

			req := httptest.NewRequest(test.method, target, nil)
			req.RequestURI = ""

			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != test.responseCode {
				t.Fatalf("expecting response status code %d but got %d", test.responseCode, resp.StatusCode)
			}
			server.Close()
		})
	}
}

func buildDummy200Handler() HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
		return nil
	}
}

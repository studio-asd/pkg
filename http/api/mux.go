package api

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/albertwidi/pkg/http/mux"
)

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

var once sync.Once

type Mux struct {
	m *mux.Mux
}

// NewMux returns the API mux wrapped with default middleware like autoResponseHandler.
func NewMux(middlewares ...mux.MiddlewareFunc) *Mux {
	m := mux.New()
	m.Use(middlewares...)
	once.Do(func() {
		m.Use(autoErrorResponseHandler)
	})
	return &Mux{m: m}
}

// RegisterAPIGroups automatically registers all API group within a groups.
func (m *Mux) RegisterAPIGroups(groups APIGroups) {
	value := reflect.ValueOf(groups).Elem()

	version := groups.Version()
	if version == "" {
		panic("cannot register api groups without version")
	}
	// Add '/' prefix if it doesn't have any prefix.
	if !strings.HasPrefix(version, "/") {
		version = "/" + version
	}

	m.route(version, func(m *Mux) {
		// Loop through all APIGroup inside of the groups to register all of them.
		for i := 0; i < value.NumField(); i++ {
			g, ok := value.Field(i).Interface().(APIGroup)
			if !ok {
				continue
			}
			m.registerAPIGroup(g)
		}
	})
}

// registerAPIGroup automatically registers all APIs inside the group using reflect. The function will loop
// through the group(struct) and check the type one by one on whether the type implements APIEndpointer or not.
func (m *Mux) registerAPIGroup(group APIGroup) {
	value := reflect.ValueOf(group).Elem()

	pathPrefix := group.PathPrefix()
	// Add '/' prefix if it doesn't have any prefix.
	if !strings.HasPrefix(pathPrefix, "/") {
		pathPrefix = "/" + pathPrefix
	}

	m.route(pathPrefix, func(m *Mux) {
		// Loop through the api group and register all endpoints.
		for i := 0; i < value.NumField(); i++ {
			ep, ok := value.Field(i).Interface().(APIEndpointer)
			if !ok {
				continue
			}
			m.handlerFunc(ep)
		}
	})
}

func (m *Mux) handlerFunc(endpoint APIEndpointer) {
	ae := endpoint.Endpoint()
	checkEndpoint(ae)

	handler := func(w http.ResponseWriter, r *http.Request) error {
		return endpoint.Handler()(w, r)
	}
	m.m.HandlerFunc(ae.Method, ae.Route, handler)
}

func (m *Mux) route(path string, fn func(m *Mux)) {
	m.m.Route(path, func(m *mux.Mux) {
		fn(&Mux{m: m})
	})
}

func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ctx := instrumentation.NewContextFromHTTPHeaders(r.Context(), r.Header)
	// if ctx != nil {
	// 	r = r.WithContext(ctx)
	// }
	m.m.ServeHTTP(w, r)
}

// checkEndpoint checks whether the endpoint is correct as we want it.
func checkEndpoint(ae APIEndpoint) {
	if reflect.ValueOf(ae).IsZero() {
		panic("api endpoint cannot be nil")
	}
	if err := ae.Validate(); err != nil {
		panic(err)
	}
}

// autoErrorResponseHandler wraps the handler and detects whether the error is a special type error.
// If the error is a special type error, then the wrapper will write the response using the information
// inside the error type.
func autoErrorResponseHandler(handler mux.HandlerFunc) mux.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		err := handler(w, r)
		if err == nil {
			return nil
		}
		delegator, ok := w.(*mux.ResponseWriterDelegator)
		if !ok {
			return err
		}
		// If the header is already written then we should avoid using the automatic response handler.
		if delegator.WroteHeader() {
			return err
		}

		var stdJson *StandardJSONResponse
		statusCode := http.StatusInternalServerError

		// Checks whether we use the internal ErrorJSON type and use the object to define the standard response.
		var errJson *ErrorJSON
		if errors.As(err, &errJson) {
			if errJson.HTTPCode != 0 {
				statusCode = errJson.HTTPCode
			}
			message := errJson.Message
			if message == nil {
				message = defaultErrorMessage
			}

			stdJson = NewJSONResponse(r.Context(), errJson.HTTPCode, message, nil)
			stdJson.Error = StandardErrorResponse{
				Message: I18nMessage{
					EN: err.Error(),
				},
				Retryable: errJson.Retryable,
			}
		} else {
			stdJson = NewJSONResponse(r.Context(), http.StatusInternalServerError, defaultErrorMessage, nil)
			stdJson.Error = StandardErrorResponse{
				Message: I18nMessage{
					EN: err.Error(),
				},
			}
		}

		w.WriteHeader(statusCode)
		if writeErr := stdJson.Write(w); writeErr != nil {
			err = errors.Join(err, writeErr)
		}
		return err
	}
}

package api

import (
	"fmt"
	"net/http"
)

// APIGroups wraps several APIGroup inside one big group. The groups are usually wrapped with a version like 'v1' so the user
// can clearly understand the version of the API they hit.
type APIGroups interface {
	// Version returns a version string like 'v1', 'v2' and so on. Version is single number as this will be used
	// as the main prefix of all APIs inside the object.
	Version() string
}

// APIGroup defines a group of endpoints inside one category. For example 'order' group that represents all orders
// APIs like 'order_detail' or 'order_summary'.
type APIGroup interface {
	// Owner is the owner of the APIGroup. In an organization, api group usually owned by
	// one team or more.
	Owner() string
	// PathPRefix of the API group. This will be the prefix of the API path of the group. For example the prefix of the api
	// group is '/order', then all APIs below the group will be '/order/_path_'.
	//
	// Please NOTE that all prefix need to be started with '/'.
	PathPrefix() string
}

// APIEndpointer defines what method are needed for a type to be described as an endpoint.
type APIEndpointer interface {
	// Endpoint returns the endpoint information for the API. The API endpoint is important to automatically
	// build the documentation from the source code.
	Endpoint() APIEndpoint
	// Handler is the implementation of API handler to serve traffic from clients.
	Handler() HandlerFunc
}

// RequestTypeResponse is an interface to force all responses to have the implement Response interface.
type RequestResponse interface {
	// internalRequestResponse is an internal api to ensure all requests response
	// are following the same pattern. And use an embedded struct to define the
	// request or response.
	internalRequestResponse()
}

// APIEndpoint defines the endpoint/API definition. This struct will be exported as the source information of API documentation.
type APIEndpoint struct {
	Title             string   // Title of the endpoint. This will be the header for endpoint document.
	Public            bool     // Public is a flag to indicate whether the API can be accessed publicly. If not then some kind of authentication is required.
	Route             string   // Route is the name of the route/path. This will also affect the muxers.
	Method            string   // Method is the HTTP method of the endpoint.
	SuccessStatusCode int      // SuccessStatusCode determine what status code to be used when the client successfully hit the endpoint.
	Request           Request  // Request is the request struct for the endpoint.
	Response          Response // Response is the response struct for the endpoint.
	Permission        string   // Permission is the permission required to hit the endpoint.
	BlockRelease      bool     // BlockRelease is a flag to mark that we should not serve this specific API. This API will be mark as 'unreleased'.
}

func (a *APIEndpoint) Validate() error {
	// Check the route first because we will use route for every other errors.
	if a.Route == "" {
		return fmt.Errorf("api_endpoint: route cannot be empty")
	}
	if a.Title == "" {
		return fmt.Errorf("api_endpoint: route %s title is empty", a.Route)
	}
	switch a.Method {
	case http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete, http.MethodOptions:
	default:
		return fmt.Errorf("api_endpoint: route %s method is empty", a.Route)
	}
	return nil
}

// Example is an example for the API request and response. The example will force the API builder to define the schema
// example for its request and response.
type Example interface {
	// The schema example is a request or response schema that will be marshalled to their
	// own structure.
	//
	// For example:
	//
	// JSON: {"key": "value"}
	// URL_PARAM: ?key=value
	// PATH_PARAM: /{key=value}
	SchemaExample() RequestResponse
}

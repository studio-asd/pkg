package api

import (
	"errors"
	"net/http"
)

type TestAPIAutoError struct{}

type TestAPIAutoErrorRequest struct {
	RequestTypeJSON
	Message string `json:"message"`
}

type TestAPIAutoErrorResponse struct {
	ResponseTypeJSON
	Message string `json:"message"`
}

func (t *TestAPIAutoErrorRequest) Validate() error {
	return nil
}

func (t *TestAPIAutoErrorRequest) SchemaExample() RequestResponse {
	return &TestAPIAutoErrorResponse{
		Message: "error",
	}
}

func (t *TestAPIAutoError) Endpoint() APIEndpoint {
	return APIEndpoint{
		Title:    "test_error",
		Method:   http.MethodGet,
		Route:    "/test_auto_error",
		Request:  &TestAPI1Request{},
		Response: &TestAPI1Response{},
	}
}

func (t *TestAPIAutoError) Handler() HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		req := &TestAPI1Request{}
		err := ReadRequest(r, req)
		if err != nil {
			return err
		}
		return errors.New(req.Message)
	}
}

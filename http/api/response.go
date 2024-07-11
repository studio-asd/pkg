package api

import (
	"context"
	"encoding/json"
	"net/http"
)

const (
	responseTypeJSON = iota + 1
)

// Response defines the response object and what methods are needed.
type Response interface {
	Example
	RequestResponse
	ResponseType() int
}

// Message defines how the message is defined and supports i18n.
// Unfortunately we have to add '_' as the prefix of the function because
// it will clash with the message itself. But it should be ok as '_' will
// only be used internally.
type Message interface {
	_ID() string                   // Indonesian error message.
	_EN() string                   // English error message.
	String(context.Context) string // Returns the message based on request context.
}

type ResponseTypeJSON struct{}

func (r ResponseTypeJSON) ResponseType() int {
	return responseTypeJSON
}

func (r ResponseTypeJSON) internalRequestResponse() {}

// jsonResponse is the internal json response that will be used to create the real
// json response to the client.
//
// Data from StandardResponse will be used to fill the jsonResponse struct.
//
// Example:
//
//	{
//		"message": "a message",
//		"data": {
//			"key1": "value1",
//			"key2": "value2"
//		},
//		"error": {
//			"retry": {
//				"retryable": true,
//				"max_retry": 3
//			}
//		}
//	}
type jsonResponse struct {
	Message string            `json:"message,omitempty"`
	Data    any               `json:"data,omitempty"`
	Error   jsonErrorResponse `json:"error,omitempty"`
}

// jsonErrorResponse defines the internal error response of a json message.
type jsonErrorResponse struct {
	Message string         `json:"message"`
	Retry   jsonErrorRetry `json:"retry,omitempty"`
	// Errors is an array of error that can be used for form validation, or in case
	// we want to show more than one errors to the user.
	Errors []jsonErrorResponse `json:"errors,omitempty"`
}

// jsonErrorRetry defines the retry mechanism that can occur during an error.
type jsonErrorRetry struct {
	Retryable bool `json:"retryable"`
	MaxRetry  int  `json:"max_retry"`
}

// StandardResponse is the default response that we will give to the user in JSON format.
//
// Implements json.Marshaler.
type StandardJSONResponse struct {
	ctx              context.Context
	httpResponseCode int
	Message          Message
	Data             Response
	Error            StandardErrorResponse
}

// StandardErrorResponse is the internal error response used by the package to construct
// the response for the user.
type StandardErrorResponse struct {
	Message   Message
	Retryable bool
}

// jsonResponse returns the jsonResponse struct as the real representative to be marshalled as
// a json message to the client.
func (sr *StandardJSONResponse) jsonResponse() *jsonResponse {
	resp := &jsonResponse{}

	if sr.Message != nil {
		resp.Message = sr.Message.String(sr.ctx)
	}
	// This means we will use the error path as the error message is not empty.
	if sr.Error.Message != nil {
		resp.Error.Message = sr.Error.Message.String(sr.ctx)
		// Set retry if the retries are possible for the specific API.
		if sr.Error.Retryable {
			resp.Error.Retry.Retryable = sr.Error.Retryable
			// For now, we only allows the maximum retry up to three times.
			resp.Error.Retry.MaxRetry = 3
		}
	}
	resp.Data = sr.Data

	return resp
}

// MarshalJSON is the implementation of the json.Marshaler and overrides the original function
// call when json.Marshal is invoked.
func (sr *StandardJSONResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(sr.jsonResponse())
}

func (sr *StandardJSONResponse) Write(w http.ResponseWriter) error {
	out, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

func NewJSONResponse(ctx context.Context, httpCode int, message Message, data Response) *StandardJSONResponse {
	resp := &StandardJSONResponse{
		ctx:              ctx,
		httpResponseCode: httpCode,
		Message:          message,
		Data:             data,
	}
	return resp
}

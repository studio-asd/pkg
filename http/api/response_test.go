package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNewErrorResponse(t *testing.T) {
	t.Parallel()

	TestNewErrorResponseMessage1 := I18nMessage{
		ID: "test_1",
		EN: "test_1",
	}
	TestNewErrorResponseMessage2 := I18nMessage{
		ID: "test_2",
		EN: "test_2",
	}

	tests := []struct {
		name               string
		httpCode           int
		message            Message
		expect             *StandardJSONResponse
		expectJSONResponse *jsonResponse
	}{
		{
			name:     "bad request",
			httpCode: http.StatusBadRequest,
			message:  TestNewErrorResponseMessage1,
			expect: &StandardJSONResponse{
				httpResponseCode: http.StatusBadRequest,
				Error: StandardErrorResponse{
					Message: TestNewErrorResponseMessage1,
				},
			},
			expectJSONResponse: &jsonResponse{
				Error: jsonErrorResponse{
					Message: TestNewErrorResponseMessage1._EN(),
				},
			},
		},
		{
			name:     "internal server error",
			httpCode: http.StatusInternalServerError,
			message:  TestNewErrorResponseMessage2,
			expect: &StandardJSONResponse{
				httpResponseCode: http.StatusInternalServerError,
				Error: StandardErrorResponse{
					Message: TestNewErrorResponseMessage2,
				},
			},
			expectJSONResponse: &jsonResponse{
				Error: jsonErrorResponse{
					Message: TestNewErrorResponseMessage2._EN(),
				},
			},
		},
	}

	for _, test := range tests {
		tt := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stdJson := NewJSONResponse(context.Background(), tt.httpCode, nil, nil)
			stdJson.Error = StandardErrorResponse{
				Message: tt.message,
			}
			if stdJson.httpResponseCode != tt.httpCode {
				t.Fatalf("expecting response code %d but got %d", tt.httpCode, stdJson.httpResponseCode)
			}

			ignoreStandardJSON := cmpopts.IgnoreFields(StandardJSONResponse{}, "httpResponseCode", "ctx")
			ignorePreferredLang := cmpopts.IgnoreUnexported(StandardErrorResponse{})
			if diff := cmp.Diff(tt.expect, stdJson, ignoreStandardJSON, ignorePreferredLang); diff != "" {
				t.Fatalf("invalid error response: (-want/+got)\n%s", diff)
			}

			if diff := cmp.Diff(tt.expectJSONResponse, stdJson.jsonResponse()); diff != "" {
				t.Fatalf("invalid jsonResponse: (-want/+got)\n%s", diff)
			}
		})
	}
}

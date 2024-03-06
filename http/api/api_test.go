package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/albertwidi/pkg/http/requestbuilder"
)

// func TestSome(t *testing.T) {
// 	groupItf := reflect.TypeOf((*APIGroup)(nil)).Elem()

// 	list := APIList{}

// 	tp := reflect.TypeOf(list)

// 	for i := 0; i < tp.NumField(); i++ {
// 		f := tp.Field(i)
// 		ftp := f.Type.Elem()
// 		fmt.Println(ftp.Name())

// 		val := reflect.New(ftp)
// 		if val.Type().Implements(groupItf) {
// 			fmt.Println("YES")
// 		}
// 		itf := val.Interface().(APIGroup)
// 		fmt.Println(itf.Owner())
// 	}
// }

var _ APIEndpointer = (*TestAPI1)(nil)

type TestAPIGroups struct {
	TestAPIGroup *TestAPIGroup
}

func (t *TestAPIGroups) Version() string {
	return "v1"
}

type TestAPIGroup struct {
	Test1         *TestAPI1
	TestAutoError *TestAPIAutoError
}

func (t *TestAPIGroup) Owner() string {
	return "test"
}

func (t *TestAPIGroup) PathPrefix() string {
	return "/testing"
}

type TestAPI1 struct{}

type TestAPI1Request struct {
	RequestTypeJSON
	Message string `json:"message"`
}

func (t *TestAPI1Request) Validate() error {
	return nil
}

func (*TestAPI1Request) SchemaExample() RequestResponse {
	return &TestAPI1Request{
		Message: "hello",
	}
}

func (*TestAPI1Request) RequestTypes() []int {
	return nil
}

type TestAPI1Response struct {
	ResponseTypeJSON
	Message string `json:"message"`
}

func (*TestAPI1Response) SchemaExample() RequestResponse {
	return &TestAPI1Response{
		Message: "halo",
	}
}

func (t *TestAPI1) Endpoint() APIEndpoint {
	return APIEndpoint{
		Title:    "test1",
		Method:   http.MethodGet,
		Route:    "/test_1",
		Request:  &TestAPI1Request{},
		Response: &TestAPI1Response{},
	}
}

func (t *TestAPI1) Handler() HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		req := &TestAPI1Request{}
		err := ReadRequest(r, req)
		if err != nil {
			return err
		}
		resp := NewJSONResponse(r.Context(), http.StatusOK, I18nMessage{EN: req.Message}, nil)
		return resp.Write(w)
	}
}

// TestAPIEndpoint test every API thta registered within the tests cases and group.
//
// For most of the tests in api package, this test alone is enough. Other tests will cover of what not being
// or can't be covered here.
func TestAPIEndpoint(t *testing.T) {
	t.Parallel()

	// Please append your test APIs here. The tes will produce error if the API is not
	// registered.
	apiGroup := &TestAPIGroup{
		Test1:         &TestAPI1{},
		TestAutoError: &TestAPIAutoError{},
	}
	groups := &TestAPIGroups{
		TestAPIGroup: apiGroup,
	}
	tests := []struct {
		Req     Request
		API     APIEndpointer
		CheckFn func(t *testing.T, httpResp *http.Response)
	}{
		{
			Req: &TestAPI1Request{
				Message: "hello",
			},
			API: &TestAPI1{},
			CheckFn: func(t *testing.T, resp *http.Response) {
				t.Helper()
				if resp.StatusCode != http.StatusOK {
					t.Fatal("not ok")
				}

				out, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()

				jresp := jsonResponse{}
				err = json.Unmarshal(out, &jresp)
				if err != nil {
					t.Fatal(err)
				}
				if jresp.Message != "hello" {
					t.Fatalf("different message from request. Expect %s but got %s", "hello", jresp.Message)
				}
			},
		},
		{
			Req: &TestAPIAutoErrorRequest{
				Message: "auto_error",
			},
			API: &TestAPIAutoError{},
			CheckFn: func(t *testing.T, resp *http.Response) {
				t.Helper()
				if resp.StatusCode != http.StatusInternalServerError {
					t.Fatalf("invalid http status code, expecting %d but got %d", http.StatusInternalServerError, resp.StatusCode)
				}

				out, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()

				jresp := jsonResponse{}
				err = json.Unmarshal(out, &jresp)
				if err != nil {
					t.Fatal(err)
				}

				expectMessage := defaultErrorMessage._EN()
				if jresp.Message != expectMessage {
					t.Fatalf("different message from request. Expect %s but got %s", expectMessage, jresp.Message)
				}
			},
		},
	}

	listener, err := net.Listen("tcp", "localhost:5872")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	httpClient := &http.Client{}

	m := NewMux()
	m.RegisterAPIGroups(groups)

	var errC chan error
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2000)
	defer cancel()

	server := &http.Server{
		ReadTimeout:  0,
		WriteTimeout: 0,
		Handler:      m,
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

	for _, test := range tests {
		tt := test
		baseURL := fmt.Sprintf("http://%s", listener.Addr().String())
		testPath, err := url.JoinPath(baseURL, groups.Version(), apiGroup.PathPrefix(), tt.API.Endpoint().Route)
		if err != nil {
			t.Fatal(err)
		}

		t.Run(testPath, func(t *testing.T) {
			t.Parallel()

			req := BuildRequest(t, tt.API.Endpoint().Method, testPath, tt.Req)
			req.Method = tt.API.Endpoint().Method
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			tt.CheckFn(t, resp)
		})
	}
}

// BuildRequest is a test helper function to create a request based on the request type object.
func BuildRequest(t *testing.T, method, reqURL string, req Request) *http.Request {
	t.Helper()

	builder := requestbuilder.New(context.Background()).
		URL(reqURL).
		Method(method)

	for _, tp := range req.RequestTypes() {
		switch tp {
		case requestTypeJSON:
			builder = builder.BodyJSON(req)
		default:
			t.Fatalf("invalid request type %d not supproted", tp)
		}
	}

	httpReq, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return httpReq
}

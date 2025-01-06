package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"github.com/gorilla/schema"

	"github.com/studio-asd/pkg/reflecthelper"
)

var (
	_ RequestResponse = (*RequestTypeJSON)(nil)
	_ RequestResponse = (*RequestTypePathParameters)(nil)
)

const (
	requestTypeJSON = iota + 1
	requestTypePathParameters
	requestTypeQueryString
	requestTypePostForm
)

// Request interface defines that a struct is a request type thus the struct should
// have a method of 'RequestType'.
type Request interface {
	Example
	RequestResponse
	// RequestTypes returns a list of request types. While most of the request types will only use one(1) type only.
	// There is a case where having multiple types in a single request beneficial. For example in a PUT request where
	// we want to combine both 'path parameters' and 'json body'. There is a real-world case example for this with GitHub API
	// https://docs.github.com/en/rest/pulls/comments?apiVersion=2022-11-28#update-a-review-comment-for-a-pull-request--parameters.
	RequestTypes() []int
	// Validate validates the user request.
	Validate() error
}

// RequestTypeJSON allows the request to be in the form of JSON within the request body.
type RequestTypeJSON struct{}

func (rbd RequestTypeJSON) RequestTypes() []int {
	return []int{requestTypeJSON}
}

func (rbd RequestTypeJSON) internalRequestResponse() {}

// RequestTypePathParameters allows the request to be in the form of path parameters.
//
// The parameters can be defined via struct tag of `path` so we can map the request
// to the variables automatically.
//
// For example:
//
//	/v1/order/{order_id}
//
//	type Request struct {
//			OrderID string `path:"order_id"`
//	}
type RequestTypePathParameters struct{}

func (r RequestTypePathParameters) RequestTypes() []int {
	return []int{requestTypePathParameters}
}

func (r RequestTypePathParameters) internalRequestResponse() {}

type RequestTypeQueryString struct{}

func (r RequestTypeQueryString) internalRequestResponse() {}

func (r RequestTypeQueryString) RequestTypes() []int {
	return []int{requestTypeQueryString}
}

type RequestTypePostForm struct{}

func (r RequestTypePostForm) internalRequestResponse() {}

func (r RequestTypePostForm) RequestTypes() []int {
	return []int{requestTypePostForm}
}

type RequestTypePathParamsAndJSON struct{}

func (r RequestTypePathParamsAndJSON) internalRequestResponse() {}

func (r RequestTypePathParamsAndJSON) RequestTypes() []int {
	return []int{requestTypePathParameters, requestTypeJSON}
}

// urlEncodedDecoder is a new gorilla that is used to decode the url-encoded request parameters.
var urlEncodedDecoder = schema.NewDecoder()

// ReadRequest reads the http request based on the request types and map them into the request struct body.
//
// For several cases, it is possible to combine several types of rqeuest type into one struct body with multiple struct tags.
// Example:
//
/*	type Example struct {
		PathParam1 string `path:"param_1"`
		PathParam2 string `path:"param_2"`
		Body stsring 	  `json:"body"`
	}
*/
// In above example we are combining both path parameters and JSON body in one struct.
func ReadRequest(r *http.Request, req Request) (err error) {
	// Loop through all request types and map all the request type into the struct body.
	// Again, this is needed because there might be several types of requests inside a single
	// api request.
	for _, t := range req.RequestTypes() {
		switch t {
		case requestTypeJSON:
			err = errors.Join(err, readJSONRequest(r, req))
		case requestTypePathParameters:
			err = errors.Join(err, readPathParamsValue(r, req))
		case requestTypeQueryString, requestTypePostForm:
			err = errors.Join(err, readURLEncodedParameters(r, req, t))
		default:
			return fmt.Errorf("invalid request type %d", t)
		}
	}
	err = req.Validate()
	return
}

// readJSONRequest reads http request body and unmarshal the body content and map it to
// struct via struct tag.
func readJSONRequest(r *http.Request, req Request) error {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return &ErrorJSON{Err: err}
	}
	defer r.Body.Close()
	err = json.Unmarshal(data, req)
	if err != nil {
		return &ErrorJSON{Err: err}
	}
	return nil
}

// readURLEncodedParameters reads url encoded parameters in the HTTP request depends on the request method.
//
// For GET request, it will read directly from url and read the query string in the URL.
// For POST request, it will read directly from post-form and read the post-form query.
func readURLEncodedParameters(r *http.Request, req any, requestType int) error {
	switch requestType {
	case requestTypeQueryString:
		switch r.Method {
		case http.MethodGet:
			queryValue := r.URL.Query()
			return urlEncodedDecoder.Decode(req, queryValue)
		case http.MethodPost:
			return fmt.Errorf("can't use query string in post request")
		}
	case requestTypePostForm:
		switch r.Method {
		case http.MethodGet:
			return fmt.Errorf("can't use post-form in get request")
		case http.MethodPost:
			// Parse the form first, without this then the r.PostForm will be empty.
			if err := r.ParseForm(); err != nil {
				return err
			}
			postFormValue := r.PostForm
			return urlEncodedDecoder.Decode(req, postFormValue)
		}
	}
	return nil
}

// readPathParamsValue set the path parameter struct tag to the request struct. We make this function to be
// separated so we can easily test the function.
func readPathParamsValue(r *http.Request, req any) error {
	op := reflect.ValueOf(req)
	if op.Kind() != reflect.Pointer {
		return fmt.Errorf("%T is not a pointer type", req)
	}
	o := reflecthelper.GetTypevalue(op)
	t := o.Type()

	if o.Kind() != reflect.Struct {
		return fmt.Errorf("%T is not a struct type", req)
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		pathKey := f.Tag.Get("path")
		value := r.PathValue(pathKey)
		if value == "" {
			continue
		}

		if !o.Field(i).CanAddr() && !o.Field(i).CanSet() {
			continue
		}

		switch f.Type.Kind() {
		case reflect.String:
			o.Field(i).SetString(value)

		case reflect.Bool:
			b, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			o.Field(i).SetBool(b)

		case reflect.Float32, reflect.Float64:
			fl, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return err
			}
			o.Field(i).SetFloat(fl)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			in, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			o.Field(i).SetInt(in)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			uin, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			o.Field(i).SetUint(uin)
		}
	}
	return nil
}

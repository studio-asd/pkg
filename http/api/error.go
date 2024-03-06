package api

var _ error = (*ErrorJSON)(nil)

// The default error message we will show to user if not message is specified.
var defaultErrorMessage = I18nMessage{
	EN: "Something is going wrong, please try again later",
	ID: "Terjadi kesalahan pada server, mohon coba kembali beberapa saat lagi",
}

// ErrorJSON allows the error response to be returned as error. The struct will be evaluated in the autoErrorResponseHandler middleware
// and checked whether the type is being used to return errors.
//
// For example:
//
//	func Handler(w http.ResponseWriter, r *http.Request) error {
//		return &ErrorJSON{}
//	}
type ErrorJSON struct {
	ResponseTypeJSON
	// Message is what we will send to the user. If no message specified, then we will
	// use the defaultErrorMessage.
	Message Message
	Err     error
	// Retryable mark the request to be retry able because of some unknown errors.
	Retryable bool
	// Code is the internal error code.
	Code string
	// HTTPCode represents the response http code. If no HTTP code present, then
	// we will use http.StatusInternalServerError code.
	HTTPCode int
}

func (e *ErrorJSON) Error() string {
	return e.Err.Error()
}

// NewErrorJSON creates a new json error response with simple status code and message. The function provide optional chaining approach to append
// more data into the error json.
func NewErrorJSON(statusCode int, message Message, options ...func(*ErrorJSON)) *ErrorJSON {
	e := &ErrorJSON{
		HTTPCode: statusCode,
		Message:  message,
	}
	for _, o := range options {
		o(e)
	}
	return e
}

// WithError injects error into ErrorJSON.
func WithError(err error) func(*ErrorJSON) {
	return func(ej *ErrorJSON) {
		ej.Err = err
	}
}

// WithCode injects code into ErrorJSON to expose the internal error code.
func WithCode(code string) func(*ErrorJSON) {
	return func(ej *ErrorJSON) {
		ej.Code = code
	}
}

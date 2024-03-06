package mux

import (
	"net/http"
)

var _ http.ResponseWriter = (*ResponseWriterDelegator)(nil)

// ResponseWriterDelagator is a http.ResponseWriter delegator.
type ResponseWriterDelegator struct {
	http.ResponseWriter
	status      int
	written     int64
	wroteHeader bool
	path        string
}

func (rwdg *ResponseWriterDelegator) Path() string {
	return rwdg.path
}

// Status return the status of WriteHeader
func (rwdg *ResponseWriterDelegator) Status() int {
	return rwdg.status
}

func (rwdg *ResponseWriterDelegator) Written() int64 {
	return rwdg.written
}

// WriteHeader via http.ResponseWriter
func (rwdg *ResponseWriterDelegator) WriteHeader(code int) {
	if rwdg.wroteHeader {
		return
	}
	rwdg.status = code
	rwdg.wroteHeader = true
	rwdg.ResponseWriter.WriteHeader(code)
}

// Write the byte via http.ResponseWriter.
func (rwdg *ResponseWriterDelegator) Write(b []byte) (int, error) {
	if !rwdg.wroteHeader {
		rwdg.WriteHeader(http.StatusOK)
	}

	n, err := rwdg.ResponseWriter.Write(b)
	rwdg.written += int64(n)
	return n, err
}

func (rwdg *ResponseWriterDelegator) WroteHeader() bool {
	return rwdg.wroteHeader
}

func newResponseWriterDelegator(path string, w http.ResponseWriter) *ResponseWriterDelegator {
	d := ResponseWriterDelegator{
		ResponseWriter: w,
		path:           path,
	}
	return &d
}

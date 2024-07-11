package api

import (
	"encoding/json"
	"net/http"
)

type HandlerHelper[Resp Response, Req Request] struct {
	req Req

	writer  http.ResponseWriter
	request *http.Request
}

func NewHandlerHelper[T Response, V Request]() *HandlerHelper[T, V] {
	return &HandlerHelper[T, V]{}
}

func (h *HandlerHelper[Resp, Req]) HandlerFunc(fn func(Req) (Resp, error)) HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		if err := ReadRequest(h.request, h.req); err != nil {
			return err
		}
		resp, err := fn(h.req)
		if err != nil {
			return err
		}
		out, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		h.writer.Write(out)
		return nil
	}
}

package client

import (
	"errors"
)

type Config struct {
	// Address follows the gRPC name resolution schema. To read more about this please read
	// https://github.com/grpc/grpc/blob/master/doc/naming.md.
	Address string
}

func (c Config) Validate() error {
	if c.Address == "" {
		return errors.New("address is empty")
	}
	return nil
}

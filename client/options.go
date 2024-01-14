package client

import (
	"time"

	"github.com/sahib/netq/server"
)

type Options struct {
	Addr        string
	ReadTimeout time.Duration
}

func DefaultOptions() Options {
	return Options{
		Addr:        server.DefaultOptions().Addr,
		ReadTimeout: 15 * time.Second,
	}
}

func (o *Options) Validate() error {
	// TODO: Implement.
	return nil
}

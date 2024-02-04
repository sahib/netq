package client

import (
	"fmt"
	"net/url"
	"time"

	"github.com/sahib/netq/protocol"
	"github.com/sahib/netq/server"
)

type Options struct {
	Addr             string
	ReadTimeout      time.Duration
	WebsocketOptions protocol.WebsocketOptions
}

func DefaultOptions() Options {
	return Options{
		Addr:             server.DefaultOptions().Addr,
		ReadTimeout:      15 * time.Second,
		WebsocketOptions: protocol.DefaultWebsocketOptions(),
	}
}

func (o *Options) Validate() error {
	u, err := url.Parse(o.Addr)
	if err != nil {
		return err
	}

	if s := u.Scheme; s != "ws" && s != "wss" {
		return fmt.Errorf("invalid scheme: %s", s)
	}

	if rt := o.ReadTimeout; rt < time.Second {
		return fmt.Errorf("read timeout should be at least 1s: %v", rt)
	}

	return nil
}

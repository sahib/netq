package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
	"github.com/sahib/timeq"
)

type Items = timeq.Items

type Client struct {
	opts    Options
	onError func(err error, canReconnect bool) bool
}

func New(opts Options) *Client {
	return &Client{
		opts: opts,
	}
}

func (c *Client) OnError(fn func(err error, canReconnect bool) bool) {
	c.onError = fn
}

func (c *Client) connect(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	extendDeadline := func() error {
		return conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	}

	// Make sure that we terminate the connection
	if err := extendDeadline(); err != nil {
		return nil, err
	}

	conn.SetPongHandler(func(_ string) error {
		fmt.Println("PONG!")
		return extendDeadline()
	})

	return conn, nil
}

func (c *Client) reconnect(ctx context.Context, err error, url string) *websocket.Conn {
	if !c.onError(err, true) {
		// no re-connect is requested.
		return nil
	}

	backoff := backoff.ExponentialBackOff{}
	backoff.MaxElapsedTime = 30 * time.Second

	for {
		// Check that the context did not fire in the mean time:
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := c.connect(ctx, url)
		if err == nil {
			return conn
		}

		// wait for some increasing time:
		wait := backoff.NextBackOff()
		if wait == backoff.Stop {
			c.onError(err, false)
			return nil
		}

		time.Sleep(wait)
	}
}

func (c *Client) buildURL(path string, params map[string]string) string {
	u, err := url.Parse(c.opts.Addr)
	if err != nil {
		panic(err) // TODO: Should not happen at this point.
	}

	u.Path = path

	var query = u.Query()
	for key, val := range params {
		query.Set(key, val)
	}

	u.RawQuery = query.Encode()
	return u.String()
}

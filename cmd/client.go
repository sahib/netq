package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/sahib/netq/client"
	"github.com/sahib/timeq"
	"github.com/urfave/cli/v2"
)

var (
	// helper to get the defaults of the server
	// (to avoid having to copy them here)
	defaultClientOpts = client.DefaultOptions()

	CommandClient = &cli.Command{
		Name:    "client",
		Aliases: []string{"c", "ctl"},
		Usage:   "Connect to a netq server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Aliases: []string{"a"},
				Usage:   "The address connect to",
				Value:   defaultClientOpts.Addr,
				EnvVars: []string{"NETQ_ADDR"},
			},
		},
		Subcommands: []*cli.Command{
			CommandClientPub,
			CommandClientSub,
		},
	}

	CommandClientPub = &cli.Command{
		Name:    "pub",
		Action:  withClient(HandleClientPub),
		Aliases: []string{"p"},
		Usage:   "Publish to a topic",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "topic",
				Aliases: []string{"t"},
				Usage:   "The topic/fork to sub",
			},
		},
	}

	CommandClientSub = &cli.Command{
		Name:    "sub",
		Action:  withClient(HandleClientSub),
		Aliases: []string{"s"},
		Usage:   "Subscribe to a topic",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "topic",
				Aliases: []string{"t"},
				Usage:   "The topic/fork to sub",
			},
			&cli.IntFlag{
				Name:    "max-messages",
				Aliases: []string{"c"},
				Usage:   "Max messages to receive before quitting; <= 0 disables",
			},
		},

		// TODO: Support setting the url query params.
		// (ack_timeout and max_unacked)
	}

	CommandClientPing = &cli.Command{
		Name:   "ping",
		Action: withClient(HandleClientPing),
		Usage:  "Ping the server",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "count",
				Aliases: []string{"c"},
				Usage:   "Stop after sending pings (0 = infinite)",
				Value:   0,
			},
		},
	}
)

func withClient(fn func(ctx *cli.Context, client *client.Client, sigCtx context.Context) error) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		sigCtx, cancel := sigContext(context.Background())
		defer cancel()

		opts := client.DefaultOptions()
		opts.Addr = ctx.String("addr")
		ctl := client.New(opts)

		ctl.OnError(func(err error, retry bool) bool {
			cancel()
			slog.Error("async client error", "err", err, "retry", retry)
			return false
		})

		// TODO: Is that necessary?
		go func() {
			<-sigCtx.Done()
			os.Stdin.Close()
		}()

		return fn(ctx, ctl, sigCtx)
	}
}

func HandleClientPub(ctx *cli.Context, ctl *client.Client, sigCtx context.Context) error {
	ackCh := make(chan uint64)
	topic := ctx.String("topic")
	pub, err := ctl.Pub(sigCtx, topic, func(id uint64) error {
		// This will be called whenever
		ackCh <- id
		return nil
	})

	if err != nil {
		return err
	}

	// Read data to push from stdin.
	// Format is one item per line,
	// with each line being KEY:VAL
	var lineIdx int
	var items client.Items
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lineIdx++
		line := scanner.Bytes()
		split := bytes.SplitN(line, []byte(":"), 2)
		if len(split) < 2 {
			return fmt.Errorf("no colon in line %d: %w", lineIdx, err)
		}

		key, err := strconv.ParseInt(string(split[0]), 10, 64)
		if err != nil {
			return fmt.Errorf("key error at line %d: %w", lineIdx, err)
		}

		cpy := make([]byte, len(split[1]))
		copy(cpy, split[1])
		items = append(items, timeq.Item{
			Key:  timeq.Key(key),
			Blob: cpy,
		})
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if len(items) == 0 {
		fmt.Println("nothing pushed")
		return nil
	}

	fmt.Println(pub.Push(items))
	select {
	case <-ackCh:
		// NOTE: we don't check for which message we got an ACk
		// as we only send a single message currently.
		return nil
	case <-time.After(15 * time.Second):
		return errors.New("waited too long for ack")
	}
}

func HandleClientSub(ctx *cli.Context, ctl *client.Client, sigCtx context.Context) error {
	msgCountCh := make(chan int, 1)
	topic := ctx.String("topic")
	sub, err := ctl.Sub(sigCtx, topic, func(batch *client.Batch) error {
		for _, item := range batch.Items {
			fmt.Printf("%d:%s\n", item.Key, item.Blob)
		}

		batch.Ack()
		msgCountCh <- len(batch.Items)
		return nil
	})

	if err != nil {
		return err
	}

	defer sub.Close()

	var totalMsgCount int
	var maxMessages = ctx.Int("max-messages")
	for {
		select {
		case <-sigCtx.Done():
			return sigCtx.Err()
		case count := <-msgCountCh:
			totalMsgCount += count
			if maxMessages > 0 && totalMsgCount > maxMessages {
				return nil
			}
		}
	}
}

func HandleClientPing(ctx *cli.Context, ctl *client.Client, sigCtx context.Context) error {
	cmdCtl, err := ctl.Cmd(sigCtx)
	if err != nil {
		return err
	}

	maxCount := ctx.Int("count")
	count := 0
	tckr := time.NewTicker(time.Second)
	for {
		select {
		case <-tckr.C:
			if err := cmdCtl.Ping(sigCtx); err != nil {
				return err
			}

			count++
			if count >= maxCount {
				return nil
			}

		case <-sigCtx.Done():
			return sigCtx.Err()
		}
	}
}

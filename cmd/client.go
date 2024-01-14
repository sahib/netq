package cmd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
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
		Name:    "client",
		Action:  HandleClientPub,
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

	// TODO: ping command?

	CommandClientSub = &cli.Command{
		Name:    "client",
		Action:  HandleClientSub,
		Aliases: []string{"s"},
		Usage:   "Subscribe to a topic",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "topic",
				Aliases: []string{"t"},
				Usage:   "The topic/fork to sub",
			},
			// TODO: option to not ack.
			// TODO: option to wait n batches before exiting.
		},
	}
)

func HandleClientPub(ctx *cli.Context) error {
	sigCtx, cancel := sigContext(context.Background())
	defer cancel()

	opts := client.DefaultOptions()
	opts.Addr = ctx.String("addr")
	ctl := client.New(opts)

	ctl.OnError(func(err error, _ bool) bool {
		cancel()
		fmt.Printf("error during pub: %v", err)
		return false
	})

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

	// Read data to push from stdin:
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

		items = append(items, timeq.Item{
			Key:  timeq.Key(key),
			Blob: split[1], // TODO: copy.
		})
	}

	if len(items) == 0 {
		fmt.Println("nothing pushed")
		return nil
	}

	pub.Push(items)
	select {
	case <-ackCh:
		// NOTE: we don't check for which message we got an ACk
		// as we only send a single message currently.
		return nil
	case <-time.After(15 * time.Second):
		return errors.New("waited too long for ack")
	}
}

func HandleClientSub(ctx *cli.Context) error {
	sigCtx, cancel := sigContext(context.Background())
	defer cancel()

	opts := client.DefaultOptions()
	opts.Addr = ctx.String("addr")
	ctl := client.New(opts)

	ctl.OnError(func(err error, _ bool) bool {
		cancel()
		fmt.Printf("error during sub: %v", err)
		return false
	})

	topic := ctx.String("topic")
	sub, err := ctl.Sub(sigCtx, topic, func(batch *client.Batch) error {
		for _, item := range batch.Items {
			fmt.Printf("%d:%s\n", item.Key, item.Blob)
		}

		batch.Ack()
		return nil
	})

	if err != nil {
		return err
	}

	<-sigCtx.Done()
	defer sub.Close()
	return nil
}

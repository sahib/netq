package cmd

import (
	"context"

	"github.com/sahib/netq/server"
	"github.com/urfave/cli/v2"
)

var (
	// helper to get the defaults of the server
	// (to avoid having to copy them here)
	defaultOpts = server.DefaultOptions()

	CommandServer = &cli.Command{
		Name:    "server",
		Action:  HandleServer,
		Aliases: []string{"s", "srv"},
		Usage:   "Start the netq server process",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Aliases: []string{"a"},
				Usage:   "The address to run the server on",
				Value:   defaultOpts.Addr,
				EnvVars: []string{"NETQ_ADDR"},
			},
			&cli.StringFlag{
				Name:    "storage-dir",
				Aliases: []string{"s"},
				Usage:   "The directory where all data is stored",
				Value:   defaultOpts.StorageDir,
				EnvVars: []string{"NETQ_STORAGE_DIR"},
			},
		},
	}
)

// TODO: Add the many other options.

func HandleServer(ctx *cli.Context) error {
	sigCtx, cancel := sigContext(context.Background())
	defer cancel()

	opts := server.DefaultOptions()
	opts.Addr = ctx.String("addr")
	opts.StorageDir = ctx.String("storage-dir")

	srv, err := server.NewServer(sigCtx, opts)
	if err != nil {
		return err
	}

	return srv.Serve()
}

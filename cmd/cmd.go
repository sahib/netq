package cmd

import (
	"github.com/urfave/cli/v2"
)

func Main(args []string) error {
	app := &cli.App{
		Name:  "netq",
		Usage: "netq command line utility",
		Commands: []*cli.Command{
			CommandServer,
		},
	}

	return app.Run(args)
}

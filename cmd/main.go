package cmd

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"
)

func Main(args []string) error {
	// make the logging a bit more colorful:
	logOut := os.Stdout

	app := &cli.App{
		Name:  "netq",
		Usage: "netq command line utility",
		Commands: []*cli.Command{
			CommandServer,
			CommandClient,
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Aliases: []string{"l"},
				Usage:   "The log level (see slog.Level)",
				Value:   "DEBUG",
				EnvVars: []string{"NETQ_LOG_LEVEL"},
			},
		},
	}

	app.Before = func(ctx *cli.Context) error {
		var level slog.Level
		if err := level.UnmarshalText([]byte(ctx.String("log-level"))); err != nil {
			return err
		}

		slog.SetDefault(slog.New(
			tint.NewHandler(logOut, &tint.Options{
				Level:      level,
				TimeFormat: time.RFC3339,
				NoColor:    !isatty.IsTerminal(logOut.Fd()),
			}),
		))

		return nil
	}

	return app.Run(args)
}

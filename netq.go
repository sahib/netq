package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/sahib/netq/cmd"
)

func main() {
	if err := cmd.Main(os.Args); err != nil {
		slog.Error(fmt.Sprintf("exit: %v", err))
		os.Exit(1)
	}
}

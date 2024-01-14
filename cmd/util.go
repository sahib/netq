package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// sigContext creates a context that will be done once
// a interrupt signal (i.e. CTRL-C) was delivered to our process.
func sigContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	go func() {
		<-sigs
		cancel()
	}()

	return ctx, cancel
}

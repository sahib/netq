package main

import (
	"fmt"
	"os"

	"github.com/sahib/netq/cmd"
)

func main() {
	if err := cmd.Main(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

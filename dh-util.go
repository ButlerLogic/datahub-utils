package main

import (
	"dhs/command"
	"dhs/util"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
)

var (
	name        string
	description string
	version     string
)

func main() {
	cmd := &command.Context{
		Debug: false,
	}

	if len(os.Args) < 2 {
		os.Args = append(os.Args, "--help")
	} else if len(os.Args) >= 2 && (util.InSlice[string]("-v", os.Args) || util.InSlice[string]("--version", os.Args)) {
		fmt.Println(version)
		return
	}

	root := &command.Root

	ctx := kong.Parse(
		root,
		kong.Name(name),
		kong.Description(description+"\nv"+version),
		kong.UsageOnError(),
	)

	ctx.Run(cmd)
}

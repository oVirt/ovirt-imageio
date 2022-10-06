// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
)

var (
	// Common flags.
	cpuprofile string

	// Sub commands.
	commands = map[string]*flag.FlagSet{}
)

func main() {
	// Add commands flagsets.

	commands["map"] = flag.NewFlagSet("map", flag.ExitOnError)
	addCommonFlags()

	// Parse command.

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: ovirt-img {%v}\n", strings.Join(commandNames(), ","))
		os.Exit(1)
	}

	cmdName := os.Args[1]
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command %v\n", cmdName)
		os.Exit(1)
	}

	// Parse command arguments.

	cmd.Parse(os.Args[2:])

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Run comamnd.

	switch cmdName {
	case "map":
		if len(cmd.Args()) != 1 {
			fmt.Fprintln(os.Stderr, "Usage: ovirt-img map [-cpuprofile=PROF] FILE|URL")
			os.Exit(1)
		}
		mapURL(cmd.Arg(0))
	default:
		panic("Unexpected error")
	}
}

func commandNames() []string {
	res := make([]string, 0, len(commands))
	for name := range commands {
		res = append(res, name)
	}
	return res
}

func addCommonFlags() {
	for _, cmd := range commands {
		cmd.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	}
}

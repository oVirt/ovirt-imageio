// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		flag.Usage()
		// TODO: Can define URL as a flag?
		fmt.Println("  URL")
		os.Exit(1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	b, err := connect(flag.Arg(0))
	if err != nil {
		log.Fatalf("%s", err)
	}
	defer b.Close()

	res, err := b.Extents()
	if err != nil {
		log.Fatalf("%s", err)
	}

	for res.Next() {
		e := res.Value()
		fmt.Printf("start=%v length=%v zero=%v\n",
			e.Start, e.Length, e.Zero)
	}
}

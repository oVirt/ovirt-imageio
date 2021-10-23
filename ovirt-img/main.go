// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		// TODO: Add upload/download commands.
		log.Fatalf("Usage: ovirt-img URL")
	}

	b, err := connect(os.Args[1])
	if err != nil {
		log.Fatalf("%s", err)
	}
	defer b.Close()

	extents, err := b.Extents()
	if err != nil {
		log.Fatalf("%s", err)
	}

	for _, e := range extents {
		fmt.Printf("start=%v length=%v zero=%v\n",
			e.Start, e.Length, e.Zero)
	}
}

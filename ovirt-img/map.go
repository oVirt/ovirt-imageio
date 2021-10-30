// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
)

func mapURL(url string) {
	b, err := connect(url)
	if err != nil {
		log.Fatalf("%s", err)
	}
	defer b.Close()

	res, err := b.Extents()
	if err != nil {
		log.Fatalf("%s", err)
	}

	// Buffer output so we print only on success.
	var out bytes.Buffer
	for res.Next() {
		e := res.Value()
		fmt.Fprintf(&out, "start=%v length=%v zero=%v\n",
			e.Start, e.Length, e.Zero)
	}
	out.WriteTo(os.Stdout)
}

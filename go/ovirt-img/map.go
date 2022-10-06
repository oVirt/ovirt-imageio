// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"

	"ovirt.org/imageio"
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

	w := bufio.NewWriterSize(os.Stdout, 32*1024)
	writeExtents(w, res)
	w.Flush()
}

// Write easy to read and compact JSON to writer.
//
// [{"start": 0, "legnth": 4096, "zero": false},
//  {"start": 4096, "legnth": 4096, "zero": true},
//  {"start": 8192, "length": 4096, "zero": false}]
//
// This uses much less memory and is much faster than creating a list of
// extents and marshaling the list. For example, writing 1,000,000 extents:
//
// method               max memory    time
// ---------------------------------------
// json.MarshalIndent      332 MiB    1.7s
// writeExtents             38 MiB    1.2s
//
func writeExtents(w io.Writer, res imageio.ExtentsResult) {
	first := true
	format := "{\"start\": %v, \"length\": %v, \"zero\": %v}"

	fmt.Fprint(w, "[")

	for res.Next() {
		e := res.Value()
		fmt.Fprintf(w, format, e.Start, e.Length, e.Zero)

		if first {
			format = ",\n " + format
			first = false
		}
	}

	fmt.Fprint(w, "]\n")
}

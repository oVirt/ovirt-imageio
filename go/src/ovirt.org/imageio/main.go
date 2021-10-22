// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import "os"
import "log"
import "fmt"
import "syscall"
import "libguestfs.org/libnbd"

const (
    segmentSize uint64 = 1024*1024*1024
)

func main() {
    if len(os.Args) != 2 {
        // TODO: Add upload/download commands.
        log.Fatalf("Usage: imageio URL")
    }

    h, err := libnbd.Create()
    if err != nil {
        log.Fatalf("%s", err)
    }
    defer func() {
        h.Shutdown(nil)
        h.Close()
    }()

    err = h.AddMetaContext("base:allocation")
    if err != nil {
        log.Fatalf("%s", err)
    }

    err = h.ConnectUri(os.Args[1])
    if err != nil {
        log.Fatalf("%s", err)
    }

    size, err := h.GetSize()
    if err != nil {
        log.Fatalf("%s", err)
    }

    for offset := uint64(0); offset < size; offset += segmentSize {
        length := min(size - offset, segmentSize)

        var extents []uint32

        cb := func(metacontext string, offset uint64, e []uint32, error *int) int {
            if *error != 0 {
                panic("expected *error == 0")
            }
            if metacontext == "base:allocation" {
                extents = e
            }
            return 0
        }

        for {
            err = h.BlockStatus(length, offset, cb, nil)
            if err == nil {
                break
            }

            // BlockStatus fails randomly, looks like bug in libnbd or the go binding.
            // https://listman.redhat.com/archives/libguestfs/2021-October/msg00113.html
            if err.(*libnbd.LibnbdError).Errno != syscall.EINTR {
                log.Fatalf("%s", err)
            }
        }

        ext_start := offset
        for i := 0; i < len(extents); i += 2 {
            ext_len := extents[i]
            ext_zero := extents[i+1] & libnbd.STATE_ZERO == libnbd.STATE_ZERO
            fmt.Printf("offset=%v length=%v zero=%v\n", ext_start, ext_len, ext_zero)
            ext_start += uint64(ext_len)
        }
    }
}

func min(a, b uint64) uint64 {
    if a < b {
        return a
    }
    return b
}

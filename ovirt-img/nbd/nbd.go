// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package nbd

import (
	"syscall"

	"libguestfs.org/libnbd"
	"ovirt.org/imageio"
	. "ovirt.org/imageio/units"
)

const (
	// The NBD protocol allows up to 2**32 - 1 (4 GiB), but large requests can
	// be slow, so we limit the size.
	MAX_EXTENT = 1 * GiB
)

type backend struct {
	h *libnbd.Libnbd
}

func Connect(url string) (*backend, error) {
	h, err := libnbd.Create()
	if err != nil {
		return nil, err
	}

	err = h.AddMetaContext("base:allocation")
	if err != nil {
		h.Close()
		return nil, err
	}

	err = h.ConnectUri(url)
	if err != nil {
		h.Close()
		return nil, err
	}

	return &backend{h: h}, nil
}

// Size return image size.
func (b *backend) Size() (uint64, error) {
	size, err := b.h.GetSize()
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Extents returns all image extents. The NBD prototcol supports getting
// partial extents, but imageio server does not support this yet.
func (b *backend) Extents() ([]*imageio.Extent, error) {
	size, err := b.Size()
	if err != nil {
		return nil, err
	}

	var result []*imageio.Extent

	for offset := uint64(0); offset < size; offset += MAX_EXTENT {
		length := min(size-offset, MAX_EXTENT)

		entries, err := b.blockStatus(offset, length)
		if err != nil {
			return nil, err
		}

		start := offset
		for i := 0; i < len(entries); i += 2 {
			length := uint64(entries[i])
			flags := entries[i+1]
			zero := (flags & libnbd.STATE_ZERO) == libnbd.STATE_ZERO

			// TODO: Merge extents with same flags.
			result = append(result, imageio.NewExtent(start, length, zero))
			start += length
		}
	}

	return result, nil
}

func (b *backend) blockStatus(offset, length uint64) ([]uint32, error) {
	var result []uint32

	cb := func(metacontext string, offset uint64, e []uint32, error *int) int {
		if *error != 0 {
			panic("expected *error == 0")
		}
		if metacontext == "base:allocation" {
			result = e
		}
		return 0
	}

	// BlockStatus may fail randomly, looks like bug in libnbd.
	// https://listman.redhat.com/archives/libguestfs/2021-October/msg00113.html
	for {
		err := b.h.BlockStatus(length, offset, cb, nil)
		if err == nil {
			break
		}
		if err.(*libnbd.LibnbdError).Errno != syscall.EINTR {
			return nil, err
		}
	}

	return result, nil
}

// Close closes the connection the NBD server. The backend cannot be used after
// closing the connection.
func (b *backend) Close() {
	b.h.Shutdown(nil)
	b.h.Close()
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

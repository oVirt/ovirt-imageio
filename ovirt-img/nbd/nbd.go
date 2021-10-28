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
	"ovirt.org/imageio/units"
)

const (
	// The NBD protocol allows up to 2**32 - 1 (4 GiB), but large requests can
	// be slow, so we limit the size.
	maxExtent = 1 * units.GiB
)

// Backend exposes a disk image served by a Network Block Device (NBD) server.
type Backend struct {
	h *libnbd.Libnbd
}

// Connect returns a connected Backend. Caller should close the backend when
// done.
func Connect(url string) (*Backend, error) {
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

	return &Backend{h: h}, nil
}

// Size return image size.
func (b *Backend) Size() (uint64, error) {
	size, err := b.h.GetSize()
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Extents returns all image extents. The NBD prototcol supports getting
// partial extents, but imageio server does not support this yet.
func (b *Backend) Extents() (imageio.ExtentsResult, error) {
	size, err := b.Size()
	if err != nil {
		return nil, err
	}

	result := &ExtentsResult{}

	for offset := uint64(0); offset < size; offset += maxExtent {
		length := min(size-offset, maxExtent)
		entries, err := b.blockStatus(offset, length)
		if err != nil {
			return nil, err
		}

		result.entries = append(result.entries, entries...)
	}

	return result, nil
}

func (b *Backend) blockStatus(offset, length uint64) ([]uint32, error) {
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

// Close closes the connection the NBD server. The Backend cannot be used after
// closing the connection.
func (b *Backend) Close() {
	b.h.Shutdown(nil)
	b.h.Close()
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// ExtentsResult iterate over extents from the NBD server, converting
// NBD pairs (length, flags) to *imageio.Extent. This minimizes
// allocations and enables merging extents during iteration.
type ExtentsResult struct {
	// {length, flags, length, flags, ...}
	// TODO: Keep multiple meta contexts.
	entries []uint32

	// Start of next value to return.
	start uint64

	// Index of next pair in entries.
	next int
}

// Next return true if there are move values.
func (r *ExtentsResult) Next() bool {
	return r.next < (len(r.entries) - 1)
}

// Value return the next extent.
// TODO: Merge extents with same flags or differnt meta context.
func (r *ExtentsResult) Value() *imageio.Extent {
	length := uint64(r.entries[r.next])
	flags := r.entries[r.next+1]
	r.next += 2

	zero := (flags & libnbd.STATE_ZERO) == libnbd.STATE_ZERO
	res := imageio.NewExtent(r.start, length, zero)
	r.start += length

	return res
}

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
	h    *libnbd.Libnbd
	size uint64
}

// Connect runs qemu-nbd and returns a connected Backend. qemu-nbd will be
// terminated when the backend is closed.
func ConnectFile(filename, format string) (*Backend, error) {
	h, err := libnbd.Create()
	if err != nil {
		return nil, err
	}

	err = h.AddMetaContext("base:allocation")
	if err != nil {
		h.Close()
		return nil, err
	}

	args := []string{
		"qemu-nbd",
		"--read-only",
		"--persistent",
		"--shared", "8",
		"--format", format,
		filename,
	}

	err = h.ConnectSystemdSocketActivation(args)
	if err != nil {
		h.Close()
		return nil, err
	}

	size, err := h.GetSize()
	if err != nil {
		return nil, err
	}

	return &Backend{h: h, size: size}, nil
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

	size, err := h.GetSize()
	if err != nil {
		return nil, err
	}

	return &Backend{h: h, size: size}, nil
}

// Size return image size.
func (b *Backend) Size() (uint64, error) {
	return b.size, nil
}

// Extents returns all image extents. The NBD prototcol supports getting
// partial extents, but imageio server does not support this yet.
func (b *Backend) Extents() (imageio.ExtentsResult, error) {
	result := &ExtentsResult{}

	// The server may return a short or long reply:
	//
	// - short reply: one of more extents, ending before the requested range.
	//   We want to consume what we got, and make more request to the server to
	//   get the rest.
	//
	// - long reply: all extents, the last extent may end after the requested
	//   range. We want to consume all the extent to minimize the number of
	//   calls to the server, and avoid duplicate work on the server side.
	//
	// In both cases we want to continue where the last extent ended.

	var offset uint64

	for offset < b.size {
		length := min(b.size-offset, maxExtent)
		entries, err := b.blockStatus(offset, length)
		if err != nil {
			return nil, err
		}

		// Collect the entries, clipping long reply and stopping if we reach
		// the end of the image. A compliant NBD server must not return an
		// extent after the end of the image, but it is easy to handle this.

		for i := 0; i < len(entries) && offset < b.size; i += 2 {
			length := uint32(min(b.size-offset, uint64(entries[i])))
			flags := entries[i+1]
			offset += uint64(length)
			result.entries = append(result.entries, length, flags)
		}
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
	// Take the current pair.
	length := uint64(r.entries[r.next])
	flags := r.entries[r.next+1]
	r.next += 2

	// Merge with next pairs with same flags.
	for r.next < len(r.entries)-1 && flags == r.entries[r.next+1] {
		length += uint64(r.entries[r.next])
		r.next += 2
	}

	zero := (flags & libnbd.STATE_ZERO) == libnbd.STATE_ZERO
	res := imageio.NewExtent(r.start, length, zero)
	r.start += length

	return res
}

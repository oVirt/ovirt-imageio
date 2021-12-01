// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation; either version 2 of the
// License, or (at your option) any later version.

package http

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"ovirt.org/imageio"
)

// Backend exposes a disk image served by imageio server on a oVirt host.
type Backend struct {
	url     string
	client  *http.Client
	size    uint64
	extents []*imageio.Extent
}

// Connect returns a connected Backend. Caller should close the backend when
// done.
func Connect(url string) (*Backend, error) {
	tr := &http.Transport{
		// TODO: Support server certificate verification.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},

		// Increass throughput from 400 MiB/s to 1.3 GiB/s
		// https://go-review.googlesource.com/c/go/+/76410.
		WriteBufferSize: 128 * 1024,

		// TODO: connection and read timeouts.
	}
	client := &http.Client{Transport: tr}

	// TODO: Send OPTIONS request

	return &Backend{url: url, client: client}, nil
}

// Size return image size.
func (b *Backend) Size() (uint64, error) {
	if b.size == 0 {
		// imageio does not expose the size of the image in the OPTIONS request
		// yet. The only way to get size is to get all the extents and compute
		// the size from the last extent.
		err := b.getExtents()
		if err != nil {
			return 0, err
		}
		last := b.extents[len(b.extents)-1]
		b.size = last.Start + last.Length
	}
	return b.size, nil
}

// Extents returns all image extents. Imageio server does not support getting
// partial extent yet.
func (b *Backend) Extents() (imageio.ExtentsResult, error) {
	if len(b.extents) == 0 {
		if err := b.getExtents(); err != nil {
			return nil, err
		}
	}
	return imageio.NewExtentsWrapper(b.extents), nil
}

// Close closes the connection to imageio server.
func (b *Backend) Close() {
	b.client.CloseIdleConnections()
}

func (b *Backend) getExtents() error {
	res, err := b.client.Get(b.url + "/extents")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return readServerError(res)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Cannot get extents: %s", err)
	}

	err = json.Unmarshal(body, &b.extents)
	if err != nil {
		return fmt.Errorf("Cannot get extents: %s", err)
	}

	return nil
}

func readServerError(res *http.Response) error {
	reason, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Error reading response: %s", err)
	}
	return fmt.Errorf("Server error: %s", reason)
}
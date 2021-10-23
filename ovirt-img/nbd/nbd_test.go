// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package nbd

import (
	"encoding/json"
	"reflect"
	"testing"

	"ovirt.org/imageio"
	. "ovirt.org/imageio/units"
)

func TestNbdSize(t *testing.T) {
	// TODO: Requires empty 6g image exposed via qemu-nbd
	b, err := Connect("nbd+unix://?socket=/tmp/nbd.sock")
	if err != nil {
		t.Fatalf("Connect failed: %s", err)
	}
	defer b.Close()

	imageSize := 6 * GiB
	size, err := b.Size()
	if err != nil {
		t.Fatalf("Size() failed: %s", err)
	} else if size != imageSize {
		t.Errorf("backend.Size() = %v, expected %v", size, imageSize)
	}
}

func TestNbdExtents(t *testing.T) {
	// TODO: Requires empty 6g image exposed via qemu-nbd
	b, err := Connect("nbd+unix://?socket=/tmp/nbd.sock")
	if err != nil {
		t.Fatalf("Connect failed: %s", err)
	}
	defer b.Close()

	extents, err := b.Extents()
	if err != nil {
		t.Fatalf("Extents() failed: %s", err)
	}

	// We don't merge extents with same flags yet.
	expected := []*imageio.Extent{
		{0 * GiB, 1 * GiB, true},
		{1 * GiB, 1 * GiB, true},
		{2 * GiB, 1 * GiB, true},
		{3 * GiB, 1 * GiB, true},
		{4 * GiB, 1 * GiB, true},
		{5 * GiB, 1 * GiB, true},
	}
	if !reflect.DeepEqual(extents, expected) {
		t.Fatalf("extents:\n%s\nexpected:\n%s\n", dump(extents), dump(expected))
	}
}

func dump(v interface{}) []byte {
	res, _ := json.MarshalIndent(v, "", " ")
	return res
}

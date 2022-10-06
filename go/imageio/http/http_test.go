// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later

package http

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"ovirt.org/imageio"
	"ovirt.org/imageio/units"
)

func TestHTTPSize(t *testing.T) {
	// TODO: Requires imageio server exposing 6 GiB empty image via nbd
	// backend.
	b, err := Connect("https://localhost:54322/images/nbd")
	if err != nil {
		t.Fatalf("Connect failed: %s", err)
	}
	defer b.Close()

	imageSize := 6 * units.GiB
	size, err := b.Size()
	if err != nil {
		t.Fatalf("Size() failed: %s", err)
	} else if size != imageSize {
		t.Errorf("backend.Size() = %v, expected %v", size, imageSize)
	}
}

func TestHTTPReadAt(t *testing.T) {
	// TODO: Requires empty 6g image exposed via qemu-nbd
	b, err := Connect("https://localhost:54322/images/nbd")
	if err != nil {
		t.Fatalf("Connect failed: %s", err)
	}
	defer b.Close()

	// Image is empty, so we expect to get zeroes.
	// TODO: Fill image with more intresting data.
	expected := make([]byte, 4096)

	// Read first block.
	if err := checkReadAt(b, 0, expected); err != nil {
		t.Error(err)
	}

	// Read block in the middle.
	if err := checkReadAt(b, int64(3*units.GiB), expected); err != nil {
		t.Error(err)
	}

	// Read last block.
	if err := checkReadAt(b, int64(6*units.GiB-4096), expected); err != nil {
		t.Error(err)
	}

	// Read block 2048 bytes after end of image.
	if err := checkReadAt(b, int64(6*units.GiB-2048), expected[:2048]); err != nil {
		t.Error(err)
	}
}

func checkReadAt(b imageio.Backend, off int64, expected []byte) error {
	buf := bytes.Repeat([]byte("x"), 4096)

	n, err := b.ReadAt(buf, off)
	if n != len(expected) {
		return fmt.Errorf("Unexpected length: %s", err)
	}

	if !bytes.Equal(buf[:n], expected) {
		return fmt.Errorf("Unexpected data: %v", hex.Dump(buf[:n]))
	}

	return nil
}

func TestHTTPExtents(t *testing.T) {
	// TODO: Requires imageio server exposing 6 GiB empty image via nbd
	// backend.
	b, err := Connect("https://localhost:54322/images/nbd")
	if err != nil {
		t.Fatalf("Connect failed: %s", err)
	}
	defer b.Close()

	res, err := b.Extents()
	if err != nil {
		t.Fatalf("Extents() failed: %s", err)
	}

	var extents []*imageio.Extent
	for res.Next() {
		extents = append(extents, res.Value())
	}

	// Imageio merges extents with same flags.
	expected := []*imageio.Extent{
		{Start: 0 * units.GiB, Length: 6 * units.GiB, Zero: true},
	}
	if !reflect.DeepEqual(extents, expected) {
		t.Fatalf("extents:\n%s\nexpected:\n%s\n", dump(extents), dump(expected))
	}
}

func dump(v interface{}) []byte {
	res, _ := json.MarshalIndent(v, "", " ")
	return res
}

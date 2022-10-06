// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later

package imageio

import "io"

// Backend exposes a disk image for transferring image data.
type Backend interface {
	io.ReaderAt

	// Size return the size of the underlying disk image.
	Size() (uint64, error)

	// Extents return image extents.
	Extents() (ExtentsResult, error)

	// Close the backend.
	Close()
}

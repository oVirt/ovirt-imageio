// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation; either version 2 of the
// License, or (at your option) any later version.

package imageio

// Backend exposes a disk image for transferring image data.
type Backend interface {

	// Size return the size of the underlying disk image.
	Size() (uint64, error)

	// Extents return image extents.
	Extents() (ExtentsResult, error)

	// Close the backend.
	Close()
}

// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later

package imageio

// Extent describes allocation info for byte range in a disk image.
type Extent struct {
	// Start is the offset of the extent from the start of the image.
	Start uint64 `json:"start"`

	// Length is the length of the extent.
	Length uint64 `json:"length"`

	// Zero means this byte range is read is zeroes. The extent may be
	// unallocated area or a zero cluster in a qcow2 image.
	Zero bool `json:"zero"`
}

// NewExtent creates a new Extent.
func NewExtent(start uint64, length uint64, zero bool) *Extent {
	return &Extent{start, length, zero}
}

// ExtentsResult iterates over extents.
type ExtentsResult interface {
	// Next returns true if there are more extents.
	Next() bool
	// Value returns the next extent.
	Value() *Extent
}

// ExtentsWraper wrapps []*Extent to provied the ExtentsResult interface.
type ExtentsWrapper struct {
	extents []*Extent
	next    int
}

// NewExtentsWrapper create new wrapper.
func NewExtentsWrapper(e []*Extent) *ExtentsWrapper {
	return &ExtentsWrapper{extents: e}
}

// Next returns true if there are move extents.
func (w *ExtentsWrapper) Next() bool {
	return w.next < len(w.extents)
}

// Value returns the next extent.
func (w *ExtentsWrapper) Value() *Extent {
	v := w.extents[w.next]
	w.next++
	return v
}

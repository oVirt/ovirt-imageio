// SPDX-FileCopyrightText: Red Hat, Inc.
// SPDX-License-Identifier: GPL-2.0-or-later

module ovirt.org/ovirt-img

// imageio-go is part of this repository.
replace ovirt.org/imageio => ../imageio

go 1.16

require (
	libguestfs.org/libnbd v1.12.0
	ovirt.org/imageio v0.0.0-00010101000000-000000000000
)

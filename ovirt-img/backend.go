// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import (
	"fmt"
	"net/url"

	"ovirt.org/imageio"
	"ovirt.org/imageio/http"
	"ovirt.org/ovirt-img/nbd"
)

func connect(s string) (imageio.Backend, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "nbd", "nbd+unix":
		return nbd.Connect(s)
	case "https":
		return http.Connect(s)
	default:
		return nil, fmt.Errorf("Unsupported URL: %s", s)
	}
}

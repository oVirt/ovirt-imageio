// ovirt-imageio
// Copyright (C) 2021 Red Hat, Inc.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"

	"ovirt.org/imageio"
	"ovirt.org/imageio/http"
	"ovirt.org/ovirt-img/nbd"
	"ovirt.org/ovirt-img/qemuimg"
)

func connect(s string) (imageio.Backend, error) {
	if isFile(s) {
		return connectFile(s)
	}
	return connectURL(s)
}

func isFile(s string) bool {
	_, err := os.Stat(s)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}

	// We canot tell.
	log.Fatal(err)
	return false
}

func connectFile(s string) (imageio.Backend, error) {
	info, err := qemuimg.Info(s)
	if err != nil {
		return nil, err
	}
	return nbd.ConnectFile(s, info.Format)
}

func connectURL(s string) (imageio.Backend, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "nbd", "nbd+unix":
		return nbd.Connect(s)
	case "https":
		return http.Connect(s)
	case "file":
		return connectFile(u.Path)
	default:
		return nil, fmt.Errorf("Unsupported URL: %s", s)
	}
}

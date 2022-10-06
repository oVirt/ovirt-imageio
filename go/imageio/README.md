<!--
SPDX-FileCopyrightText: Red Hat, Inc.
SPDX-License-Identifier: LGPL-2.1-or-later
-->

# oVirt imageio Go client

This library provides client for the ovirt-imageio server for the Go
language.

Usage example:

    import (
        "fmt"
        "io"
        "ovirt.org/imageio"
    )

    // SimpleUpload uploads an entire disk image to imageio server.
    func SimpleUpload(image io.Reader, size int64, url string)  error {
        disk, err := imageio.Connect(url)
        if err != nil {
            return err
        }

        defer disk.Close()

        // Send a PUT request reading size bytes from image
        if err := disk.ReadFromN(image, size); err != nil {
            return err
        }

        // Flush data to underlying storage.
        if err := disk.Flush(); err != nil {
            return err
        }

        return nil
    }

    // SimpleDownload downloads an entire disk from imageio server.
    func SimpleDownload(url string, image io.Writer)  error {
        disk, err := imageio.Connect(url)
        if err != nil {
            return err
        }

        defer disk.Close()

        // Send a GET request, writing received data to image.
        if _, err := disk.WriteTo(image); err != nil {
            return err
        }

        return nil
    }

## LICENSE

The is free software licensed under the GNU Lesser General Public
License version 2 or above (LGPLv2+). See the file LICENSE for details.

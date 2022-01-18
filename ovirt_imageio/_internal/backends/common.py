# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.


class Closed:

    def __getattr__(self, name):
        """
        Behave like closed io.FileIO.
        """
        raise ValueError("Operation on closed backend")


CLOSED = Closed()

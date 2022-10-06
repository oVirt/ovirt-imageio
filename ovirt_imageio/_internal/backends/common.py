# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

class Closed:

    def __getattr__(self, name):
        """
        Behave like closed io.FileIO.
        """
        raise ValueError("Operation on closed backend")


CLOSED = Closed()

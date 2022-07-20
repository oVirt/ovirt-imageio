# ovirt-imageio
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

import sys
import threading
import time

from .. _internal import util


class ProgressBar:

    def __init__(self, size=0, output=sys.stdout, step=None, width=79,
                 now=time.monotonic):
        """
        Argumnets:
            size (int): total number of bytes. If size is unknown when
                creating, progress value is not displayed. The size can be set
                later to enable progress display.
            output (fileobj): file to write progress to (default sys.stdout).
            step (float): unused, kept for backward compatibility. The progress
                is updated in 1 percent steps.
            width (int): width of progress bar in characters (default 79)
            now (callable): callable returning current time for testing.
        """
        self.size = size
        self.output = output
        # TODO: use current terminal width instead.
        self.width = width
        self.now = now
        self.lock = threading.Lock()
        self.start = self.now()
        # Number of bytes transferred.
        self.done = 0
        # Value in percent. We start with -1 so the first update will show 0%.
        self.value = -1

        # The first update can take some time.
        self._draw()

    def update(self, n):
        """
        Increment the progress by n bytes.

        Should be called for every successful transfer, with the number of
        bytes transferred. The number of bytes transferred is updated on every
        call, but the progress is drawn only when the value changes.

        Note: this interface is compatible with tqdm[1], to allow user to use
        different progress implementations.

        [1] https://github.com/tqdm/tqdm#manual
        """
        with self.lock:
            self.done += n
            if self.size:
                new_value = int(self.done / self.size * 100)
                if new_value > self.value:
                    self.value = new_value
                    self._draw()

    def close(self):
        with self.lock:
            # If we wrote progress, we need to draw the last progress line.
            if self.done > 0:
                self._draw(last=True)

    def _draw(self, last=False):
        elapsed = self.now() - self.start

        if self.size:
            progress = "%3d%%" % max(0, self.value)
        else:
            progress = "----"

        line = "[ %s ] %s, %.2f seconds, %s/s" % (
            progress,
            util.humansize(self.done),
            elapsed,
            util.humansize(self.done / elapsed if elapsed else 0),
        )

        line = line.ljust(self.width, " ")

        # Using "\r" moves the cursor to the first column, so the next progress
        # will overwite this one. If this is the last progress, we use "\n" to
        # move to the next line. Otherwise, the next shell prompt will include
        # part of the old progress.
        end = "\n" if last else "\r"

        self.output.write(line + end)
        self.output.flush()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

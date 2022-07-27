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

    def __init__(self, phase=None, error_phase="command failed", size=None,
                 output=sys.stdout, step=None, width=79, now=time.monotonic):
        """
        Arguments:
            phase (str): short description of the current phase.
            error_phase (str): phase to set when code run under the context
                manager has failed.
            size (int): total number of bytes. If size is unknown when
                creating, progress value is not displayed. The size can be set
                later to enable progress display.
            output (fileobj): file to write progress to (default sys.stdout).
            step (float): unused, kept for backward compatibility. The progress
                is updated in 1 percent steps.
            width (int): width of progress bar in characters (default 79)
            now (callable): callable returning current time for testing.
        """
        self._phase = phase
        self._error_phase = error_phase
        self._size = size
        self._output = output
        # TODO: use current terminal width instead.
        self._width = width
        self._now = now
        self._lock = threading.Lock()
        self._start = self._now()
        # Number of bytes transferred.
        self._done = 0
        # Value in percent. We start with -1 so the first update will show 0%.
        self._value = -1
        self._closed = False

        # The first update can take some time.
        self._draw()

    @property
    def phase(self):
        return self._phase

    @phase.setter
    def phase(self, s):
        with self._lock:
            if self._closed:
                return
            if self._phase != s:
                self._phase = s
                self._draw()

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, n):
        with self._lock:
            if self._closed:
                return
            if self._size != n:
                self._size = n
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
        with self._lock:
            if self._closed:
                return
            self._done += n
            if self._size:
                new_value = int(self._done / self._size * 100)
                if new_value > self._value:
                    self._value = new_value
                    self._draw()

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                self._draw(last=True)

    def _draw(self, last=False):
        elapsed = self._now() - self._start
        progress = f"{max(0, self._value):3d}%" if self._size else "----"
        done = util.humansize(self._done)
        rate = util.humansize((self._done / elapsed) if elapsed else 0)
        phase = f" | {self._phase}" if self._phase else ""
        line = f"[ {progress} ] {done}, {elapsed:.2f} s, {rate}/s{phase}"
        line = line.ljust(self._width, " ")

        # Using "\r" moves the cursor to the first column, so the next progress
        # will overwrite this one. If this is the last progress, we use "\n" to
        # move to the next line. Otherwise, the next shell prompt will include
        # part of the old progress.
        end = "\n" if last else "\r"

        self._output.write(line + end)
        self._output.flush()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # If an exception was raised in the caller code, show a failure.
        if t is not None:
            self.phase = self._error_phase
        self.close()

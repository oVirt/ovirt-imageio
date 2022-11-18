# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

import json
import sys
import threading
import time

from .. _internal import util


FORMAT_TEXT = "text"
FORMAT_JSON = "json"

DEFAULT_WIDTH = 79


class OutputFormat:

    def __init__(self, output, width=DEFAULT_WIDTH):
        """
        Arguments:
            output (fileobj): file to write progress to (default sys.stdout).
            width (int): width of progress bar in characters (default 79)
        """
        self._output = output
        # TODO: use current terminal width instead.
        self._width = width

    def _write_line(self, line, end="\n"):
        self._output.write(line + end)
        self._output.flush()

    def draw(self, elapsed, value, transferred, size=None,
             phase=None, last=False):
        raise NotImplementedError


class TextFormat(OutputFormat):

    def draw(self, elapsed, value, transferred, size=None,
             phase=None, last=False):
        progress = f"{max(0, value):3d}%" if size else "----"
        done = util.humansize(transferred)
        rate = util.humansize((transferred / elapsed) if elapsed else 0)
        phase = f" | {phase}" if phase else ""
        line = f"[ {progress} ] {done}, {elapsed:.2f} s, {rate}/s{phase}"

        # Using "\r" moves the cursor to the first column, so the next progress
        # will overwrite this one. If this is the last progress, we use "\n" to
        # move to the next line. Otherwise, the next shell prompt will include
        # part of the old progress.
        end = "\n" if last else "\r"

        line = line.ljust(self._width, " ")
        self._write_line(line, end)


class JsonFormat(OutputFormat):

    def draw(self, elapsed, value, transferred, size=None,
             phase=None, last=False):
        progress = {
            'transferred': transferred,
            'elapsed': elapsed,
            'description': phase or "",
        }
        if size is not None:
            progress["size"] = size

        line = json.dumps(progress)
        self._write_line(line)


OUTPUT_FORMAT = {
    FORMAT_TEXT: TextFormat,
    FORMAT_JSON: JsonFormat,
}


class ProgressBar:

    def __init__(self, phase=None, error_phase="command failed", size=None,
                 output=sys.stdout, format=FORMAT_TEXT, step=None,
                 width=DEFAULT_WIDTH, now=time.monotonic):
        """
        Arguments:
            phase (str): short description of the current phase.
            error_phase (str): phase to set when code run under the context
                manager has failed.
            size (int): total number of bytes. If size is unknown when
                creating, progress value is not displayed. The size can be set
                later to enable progress display.
            output (fileobj): file to write progress to (default sys.stdout).
            format (str): format in which the progress is printed.
            step (float): unused, kept for backward compatibility. The progress
                is updated in 1 percent steps.
            width (int): width of progress bar in characters (default 79)
            now (callable): callable returning current time for testing.
        """
        self._phase = phase
        self._error_phase = error_phase
        self._size = size
        self._output = output
        self._format = OUTPUT_FORMAT[format](output=output, width=width)
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
        self._format.draw(
            elapsed=self._now() - self._start,
            value=self._value,
            transferred=self._done,
            size=self._size,
            phase=self._phase,
            last=last
        )

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # If an exception was raised in the caller code, show a failure.
        if t is not None:
            self.phase = self._error_phase
        self.close()

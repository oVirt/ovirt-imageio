# ovirt-imageio
# Copyright (C) 2022 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

"""
Client application global state.
"""

import signal

_termination_signal = None


class TerminatedBySignal(Exception):

    def __init__(self, signo):
        self.signal = signo

    def __str__(self):
        return f"Terminated by signal {self.signal}"


def check_terminated():
    """
    Raise TerminatedBySignal if termination signal was received.
    """
    if _termination_signal is not None:
        raise TerminatedBySignal(_termination_signal)


def setup_signals():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


def _handle_signal(signo, frame):
    global _termination_signal
    if _termination_signal is None:
        _termination_signal = signo

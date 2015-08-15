"""
This module provides a monotonic time. It's a time which doesn't depend on the
system time which means is not influenced by time shifts.
"""
import ctypes
import os
import logging

__all__ = ["time"]

logger = logging.getLogger(__name__)

# this means time not influenced by ntp corrections - see <linux/time.h>
CLOCK_MONOTONIC_RAW = 4


class Timespec(ctypes.Structure):
    _fields_ = [
        ('tv_sec', ctypes.c_long),
        ('tv_nsec', ctypes.c_long)
    ]

_have_monotonic = False

try:
    librt = ctypes.CDLL('librt.so.1', use_errno=True)
    clock_gettime = librt.clock_gettime
    clock_gettime.argtypes = [ctypes.c_int, ctypes.POINTER(Timespec)]
    _have_monotonic = True
except OSError as ex:
    logger.warning(("Can't use monotonic time, timeouts will be influenced by"
                   " time shifts: '{0}'").format(ex))


def time():
    if _have_monotonic is False:
        import time
        return int(time.time())

    t = Timespec()
    if clock_gettime(CLOCK_MONOTONIC_RAW, ctypes.pointer(t)) != 0:
        errno_ = ctypes.get_errno()
        raise OSError(errno_, os.strerror(errno_))
    return t.tv_sec

/*
 * ovirt-imageio
 * Copyright (C) 2017-2018 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
*/

#include <Python.h>

#define GNU_SOURCE
#include <fcntl.h>
#include <linux/falloc.h>  /* For FALLOC_FL_* on RHEL, glibc < 2.18 */
#include <sys/ioctl.h>  /* ioctl */
#include <linux/fs.h>   /* BLKZEROOUT */

PyDoc_STRVAR(blkzeroout_doc, "\
blkzeroout(fd, offset, length)\n\
Zero-fill a byte range on a block device, either using hardware offload\n\
or by explicitly writing zeroes to the device.\n\
\n\
Arguments\n\
  fd (int):      file descriptor open for write on a block device\n\
  offset (int):  start of range\n\
  length (int):  length of range\n\
\n\
Raises\n\
  OSError if the oprartion failed.\n\
");

static PyObject *
blkzeroout(PyObject *self, PyObject *args, PyObject *kw)
{
    char *keywords[] = {"fd", "start", "length", NULL};
    int fd;
    uint64_t range[2];
    int err;

    if (!PyArg_ParseTupleAndKeywords(args, kw, "iKK:blkzeroout", keywords,
                &fd, &range[0], &range[1]))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    err = ioctl(fd, BLKZEROOUT, &range);
    Py_END_ALLOW_THREADS

    if (err != 0)
        return PyErr_SetFromErrno(PyExc_OSError);

    Py_RETURN_NONE;
}

PyDoc_STRVAR(blksszget_doc, "\
blksszget(fd)\n\
Return block device logical block size.\n\
\n\
Arguments\n\
  fd (int):      file descriptor open for read on block device\n\
\n\
Raises\n\
  OSError if the oprartion failed.\n\
\n\
Returns\n\
  block size (int)\n\
");

static PyObject *
blksszget(PyObject *self, PyObject *args)
{
    int fd;
    int res;
    int err;

    if (!PyArg_ParseTuple(args, "i:blksszget", &fd))
        return NULL;

    /* This should not block but lets not take risk. */
    Py_BEGIN_ALLOW_THREADS
    err = ioctl(fd, BLKSSZGET, &res);
    Py_END_ALLOW_THREADS

    if (err != 0)
        return PyErr_SetFromErrno(PyExc_OSError);

    return PyLong_FromLong(res);
}

PyDoc_STRVAR(is_zero_doc, "\
is_zero(buf)\n\
Return True if buf is full of zeros.\n\
\n\
Arguments\n\
  buf (buffer):  buffer to check\n\
");

static PyObject *
is_zero(PyObject *self, PyObject *args)
{
    Py_buffer b;
    const unsigned char *p;
    size_t i;
    int res;

    if (!PyArg_ParseTuple(args, "s*:is_zero", &b))
        return NULL;

    /*
     * Based on Rusty Russell's memeqzero.
     *
     * dd is using a fancier version, optimized for very small bufferes.
     * In the context of imageio, we care only about big buffers, so we
     * use the origianl simpler and elegant version.
     *
     * See http://rusty.ozlabs.org/?p=560 for more info.
     */

    p = b.buf;

    /* Check first 16 bytes manually. */
    for (i = 0; i < 16; i++) {
        if (b.len == 0) {
            res = 1;
            goto out;
        }

        if (*p) {
            res = 0;
            goto out;
        }

        p++;
        b.len--;
    }

    /* Now we know that's zero, memcmp with self. */
    res = memcmp(b.buf, p, b.len) == 0;

out:
    PyBuffer_Release(&b);

    return PyBool_FromLong(res);
}

PyDoc_STRVAR(py_fallocate_doc, "\
fallocate(fd, mode, offset, length)\n\
Allows the caller to directly manipulate the allocated disk space for\n\
the file referred to by fd for the byte range starting at offset and\n\
continuing for length bytes.\n\
\n\
Arguments\n\
  fd        file descriptor to operated on (int)\n\
  mode      the operation to be performed on the given range (int)\n\
  offset    start of range (int)\n\
  length    length of range (int)\n\
\n\
Modes\n\
  FALLOC_FL_KEEP_SIZE       file size will not be changed by the\n\
                            operation\n\
  FALLOC_FL_PUNCH_HOLE      deallocates space (i.e., creates a hole) in\n\
                            the given byte range.\n\
  FALLOC_FL_COLLAPSE_RANGE  removes a byte range from a file, without\n\
                            leaving a hole.\n\
  FALLOC_FL_ZERO_RANGE      zeroes space in the byte range.\n\
\n\
Raises\n\
  OSError if the oprartion failed.\n\
\n\
See FALLOCATE(2) for more info.\n\
");

static PyObject *
py_fallocate(PyObject *self, PyObject *args)
{
    int fd;
    int mode;
    off_t offset;
    off_t length;
    int err;

    if (!PyArg_ParseTuple(args, "iiLL", &fd, &mode, &offset, &length))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    err = fallocate(fd, mode, offset, length);
    Py_END_ALLOW_THREADS

    if (err != 0)
        return PyErr_SetFromErrno(PyExc_OSError);

    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"blkzeroout", (PyCFunction) blkzeroout, METH_VARARGS | METH_KEYWORDS,
        blkzeroout_doc},
    {"blksszget", (PyCFunction) blksszget, METH_VARARGS, blksszget_doc},
    {"is_zero", (PyCFunction) is_zero, METH_VARARGS, is_zero_doc},
    {"fallocate", (PyCFunction) py_fallocate, METH_VARARGS, py_fallocate_doc},
    {NULL}  /* Sentinel */
};

static int module_init(PyObject *m)
{
    if (PyModule_AddIntConstant(m, "FALLOC_FL_KEEP_SIZE", FALLOC_FL_KEEP_SIZE))
        return -1;

    if (PyModule_AddIntConstant(m, "FALLOC_FL_PUNCH_HOLE", FALLOC_FL_PUNCH_HOLE))
        return -1;

    if (PyModule_AddIntConstant(m, "FALLOC_FL_COLLAPSE_RANGE", FALLOC_FL_COLLAPSE_RANGE))
        return -1;

    if (PyModule_AddIntConstant(m, "FALLOC_FL_ZERO_RANGE", FALLOC_FL_ZERO_RANGE))
        return -1;

    return 0;
}

#define MODULE_NAME "ioutil"
#define MODULE_DOC "Low level I/O utilities"

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    MODULE_DOC,
    -1,
    module_methods,
};

PyMODINIT_FUNC
PyInit_ioutil(void)
{
    PyObject *m;

    m = PyModule_Create(&moduledef);
    if (m == NULL)
        return NULL;

    if (module_init(m))
        return NULL;

    return m;
}

#else

PyMODINIT_FUNC
initioutil(void)
{
    PyObject *m;

    m = Py_InitModule3(MODULE_NAME, module_methods, MODULE_DOC);
    if (m == NULL)
        return;

    module_init(m);
}

#endif

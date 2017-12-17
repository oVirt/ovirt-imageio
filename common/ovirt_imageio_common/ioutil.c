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

static PyMethodDef module_methods[] = {
    {"blkzeroout", (PyCFunction) blkzeroout, METH_VARARGS | METH_KEYWORDS,
        blkzeroout_doc},
    {"is_zero", (PyCFunction) is_zero, METH_VARARGS, is_zero_doc},
    {NULL}  /* Sentinel */
};

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
    return PyModule_Create(&moduledef);
}

#else

PyMODINIT_FUNC
initioutil(void)
{
    Py_InitModule3(MODULE_NAME, module_methods, MODULE_DOC);
}

#endif

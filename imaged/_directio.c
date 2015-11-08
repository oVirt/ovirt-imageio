/*
 * vdsm-imaged - vdsm image daemon
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h> /* offsetof */

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

/* The O_DIRECT flag may impose alignment restrictions on the length and
 * address of user-space buffers and the file offset of I/Os.  In Linux
 * alignment restrictions vary  by  filesystem  and kernel version and might be
 * absent entirely.  However there is currently no filesystem-independent
 * interface for an application to discover these restrictions for a given file
 * or filesystem.  Since Linux 2.6.0, alignment to the logical block size of
 * the underlying storage (typically 512 bytes) suffices. */
#define BLOCK_SIZE 512

/* Helpers */

static PyObject *
set_error_info(int err, const char *msg, const char *file, int line)
{
    char buf[128];
    PyObject *value;

    snprintf(buf, sizeof(buf), "%s: %s (%s:%d)",
             msg, strerror(err), file, line);

    value = Py_BuildValue("(is)", err, buf);
    if (value == NULL)
        return NULL;

    PyErr_SetObject(PyExc_OSError, value);
    Py_CLEAR(value);

    return NULL;
}

#define set_error(err, msg) set_error_info(err, msg, __FILE__, __LINE__)

/* Buffer object */

typedef struct {
    PyObject_HEAD
    void *data;
    size_t size;
    size_t pos;
    PyObject *weakrefs;
} bufferobj;

PyDoc_STRVAR(buffer_doc,
"Buffer(size, align=512)");

static int
buffer_init(bufferobj *self, PyObject *args, PyObject *kwds)
{
    size_t size = 0;
    size_t align = BLOCK_SIZE;
    static char *kwlist[] = {"size", "align", NULL};
    int err;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "I|I", kwlist, &size, &align))
        return -1;

    if (size == 0 || size % BLOCK_SIZE) {
        PyErr_Format(PyExc_ValueError,
                     "size must be non-zero multiple of %d bytes",
                     BLOCK_SIZE);
        return -1;
    }

    if (align == 0 || align % BLOCK_SIZE) {
        PyErr_Format(PyExc_ValueError,
                     "align must be non-zero multiple of %d bytes",
                     BLOCK_SIZE);
        return -1;
    }

    self->size = size;
    self->pos = 0;

    free(self->data);
    self->data = NULL;

    err = posix_memalign(&self->data, align, size);
    if (err) {
        set_error(errno, "posix_memalign");
        return -1;
    }

    return 0;
}

static void
buffer_dealloc(bufferobj *self)
{
    if (self->weakrefs)
        PyObject_ClearWeakRefs((PyObject *) self);

    free(self->data);
    PyObject_Del(self);
}

static PyObject *
buffer_copyfrom(bufferobj *self, PyObject *args)
{
    char *data;
    size_t length;

    if (!PyArg_ParseTuple(args, "s#:copyfrom", &data, &length))
        return NULL;

    if (length > self->size) {
        PyErr_SetString(PyExc_ValueError, "data out of range");
        return NULL;
    }

    memcpy(self->data, data, length);
    self->pos = length;

    return PyInt_FromSize_t(self->pos);
}

static PyObject *
buffer_readfrom(bufferobj *self, PyObject *args, PyObject *kwds)
{
    int fd = -1;
    size_t count = self->size;
    static char *kwlist[] = {"fd", "count", NULL};
    ssize_t n;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i|I", kwlist, &fd, &count))
        return NULL;

    if (count > self->size) {
        PyErr_SetString(PyExc_ValueError, "count out of range");
        return NULL;
    }

    if (count == 0 || count % BLOCK_SIZE) {
        PyErr_Format(PyExc_ValueError,
                     "count must be non-zero multiple of %d bytes",
                     BLOCK_SIZE);
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;

    do {
        n = read(fd, self->data, count);
    } while (n < 0 && errno == EINTR);

    Py_END_ALLOW_THREADS;

    if (n < 0) {
        set_error(errno, "read");
        return NULL;
    }

    self->pos = n;

    return PyInt_FromSize_t(self->pos);
}

/* Functions for treating a bufferobj as a buffer */

static int
validate_index(Py_ssize_t index)
{
    if (index != 0) {
        PyErr_SetString(PyExc_SystemError,
                        "Accessing non-existent buffer segment");
        return -1;
    }
    return 0;
}

static Py_ssize_t
buffer_getreadbuf(bufferobj *self, Py_ssize_t index, const void **ptr)
{
    if (validate_index(index))
        return -1;
    *ptr = self->data;
    return self->pos;
}

static Py_ssize_t
buffer_getwritebuf(bufferobj *self, Py_ssize_t index, const void **ptr)
{
    if (validate_index(index))
        return -1;
    *ptr = self->data;
    return self->size;
}

static Py_ssize_t
buffer_getsegcount(bufferobj *self, Py_ssize_t *lenp)
{
    if (lenp)
        *lenp = self->pos;
    return 1;
}

static Py_ssize_t
buffer_getcharbuffer(bufferobj *self, Py_ssize_t index, const void **ptr)
{
    if (validate_index(index))
        return -1;
    *ptr = (const char *)self->data;
    return self->pos;
}

static PyObject *
buffer_str(bufferobj *self)
{
    return PyString_FromStringAndSize((const char *)self->data, self->pos);
}

static PyBufferProcs buffer_as_buffer = {
    (readbufferproc)buffer_getreadbuf,
    (writebufferproc)buffer_getwritebuf,
    (segcountproc)buffer_getsegcount,
    (charbufferproc)buffer_getcharbuffer,
};

static PyMethodDef buffer_methods[] = {
    {"readfrom", (PyCFunction)buffer_readfrom,  METH_VARARGS | METH_KEYWORDS, NULL},
    {"copyfrom", (PyCFunction)buffer_copyfrom,  METH_VARARGS, NULL},
    {NULL}  /* Sentinel */
};

static PyTypeObject BufferType = {
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
    "_directio.Buffer",         /* tp_name */
    sizeof(bufferobj),          /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)buffer_dealloc, /* tp_dealloc */
    0,                          /* tp_print */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_compare */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    (reprfunc)buffer_str,       /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    &buffer_as_buffer,          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    buffer_doc,                 /* tp_doc */
    0,                          /* tp_traverse */
    0,                          /* tp_clear */
    0,                          /* tp_richcompare */
    offsetof(bufferobj, weakrefs),  /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    buffer_methods,             /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    (initproc)buffer_init,      /* tp_init */
    0,                          /* tp_alloc */
    0,                          /* tp_new */
};

PyDoc_STRVAR(module_doc,
"Copyright 2015 Red Hat, Inc.  All rights reserved.\n\
\n\
This copyrighted material is made available to anyone wishing to use,\n\
modify, copy, or redistribute it subject to the terms and conditions\n\
of the GNU General Public License v2 or (at your option) any later version.");

static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

PyMODINIT_FUNC
init_directio(void)
{
    PyObject* module;

    BufferType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BufferType) < 0)
        return;

    module = Py_InitModule3("_directio", module_methods, module_doc);

    Py_INCREF(&BufferType);
    PyModule_AddObject(module, "Buffer", (PyObject *)&BufferType);
}

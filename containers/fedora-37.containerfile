# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

FROM registry.fedoraproject.org/fedora:37

# Runtime packages.
RUN echo v1 \
    && dnf install -y dnf-plugins-core \
    && dnf copr -y enable @virtmaint-sig/virt-preview \
    && dnf install -y \
        createrepo_c \
        e2fsprogs \
        gcc \
        git \
        libcurl-devel \
        libguestfs-tools-c \
        libxml2-devel \
        make \
        openssl \
        openssl-devel \
        python3-devel \
        python3-pip \
        python3-setuptools \
        python3-systemd \
        qemu-img \
        qemu-kvm \
        rpm-build \
        sudo \
        systemd-udev \
        util-linux \
        xfsprogs \
    && depmod $(basename /lib/modules/*) \
    && dnf clean all

# Developemnt tools.
COPY requirements.txt requirements.txt
RUN python3 -m pip install --upgrade pip \
    && python3 -m venv /venv \
    && /venv/bin/python3 -m pip install --requirement requirements.txt \
    && /venv/bin/python3 -m pip install ovirt-engine-sdk-python

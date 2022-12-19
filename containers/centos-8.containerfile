# SPDX-FileCopyrightText: Red Hat, Inc.
# SPDX-License-Identifier: GPL-2.0-or-later

FROM quay.io/centos/centos:stream8

# Runtime packages.
RUN echo v1 \
    && dnf install -y dnf-plugins-core \
    && dnf copr enable -y \
        ovirt/ovirt-master-snapshot \
        centos-stream-8 \
    && dnf install -y ovirt-release-master \
    && dnf install -y \
        createrepo_c \
        e2fsprogs \
        gcc \
        git \
        libguestfs-tools-c \
        make \
        openssl \
        python3-devel \
        python3-ovirt-engine-sdk4 \
        python3-pip \
        python3-setuptools \
        python3-systemd \
        python39-devel \
        python39-ovirt-engine-sdk4 \
        python39-pip \
        python39-setuptools \
        qemu-img \
        qemu-kvm \
        rpm-build \
        sudo \
        systemd-devel \
        systemd-udev \
        util-linux \
        xfsprogs \
    && dnf clean all

# Developemnt tools.
COPY requirements.txt requirements.txt
RUN python3 -m pip install --upgrade pip \
    && python3 -m venv /venv \
    && /venv/bin/python3 -m pip install --requirement requirements.txt

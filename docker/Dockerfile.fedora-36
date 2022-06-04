FROM registry.fedoraproject.org/fedora:36

# Runtime packages.
RUN echo v1 \
    && dnf install -y dnf-plugins-core \
    && dnf copr -y enable @virtmaint-sig/virt-preview \
    && dnf install -y \
        createrepo_c \
        e2fsprogs \
        gcc \
        git \
        libguestfs-tools-c \
        make \
        openssl \
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
    && python3 -m pip install --requirement requirements.txt

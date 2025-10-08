# SPDX-FileCopyrightText: 2023 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG EOS_VERSION=master
RUN set -ex; \
    echo "EOS version is ${EOS_VERSION}";

COPY ./etc/yum.repos.d/ /etc/yum.repos.d/

RUN mkdir -p shared;

RUN set -ex; \
    dnf -y update; \
    dnf -y install epel-release almalinux-release-devel; \
    dnf -y install git vim rsync cmake3 dnf-utils ccache openssh-server; \
    dnf -y group install "Development Tools";

RUN set -ex; \
    git clone https://gitlab.cern.ch/dss/eos.git eos; \
    cd eos; \
    git checkout ${EOS_VERSION}; \
    git submodule sync --recursive && git submodule update --init --recursive;

RUN set -ex; \
    mkdir build_srpm; \
    cd build_srpm; \
    cmake3 -DPACKAGEONLY:Bool=true ../eos; \
    make srpm;

RUN set -ex; \
    dnf builddep -y --nogpgcheck build_srpm/SRPMS/*;

RUN set -ex; \
    rm -rf build_srpm;

# Setup SSH server for remote access
RUN /usr/bin/ssh-keygen -A; \
    rm /run/nologin;

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

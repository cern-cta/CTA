# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock

RUN dnf install -y gcc gcc-c++ cmake3 rpm-build dnf-utils make ninja-build ccache systemd-devel clang-tools-extra libasan && \
    dnf clean all --enablerepo=\*
# ENV PATH=/opt/rh/gcc-toolset-14/root/usr/bin:$PATH
# ENV LD_LIBRARY_PATH=/opt/rh/gcc-toolset-14/root/usr/lib64:$LD_LIBRARY_PATH
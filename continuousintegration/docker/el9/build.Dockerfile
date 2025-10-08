# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock

RUN dnf install -y gcc gcc-c++ cmake3 rpm-build dnf-utils make ninja-build ccache systemd-devel && \
    dnf clean all --enablerepo=\*

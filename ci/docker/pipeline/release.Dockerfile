# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y epel-release almalinux-release-devel && \
    dnf install -y git curl jq krb5-workstation rsync openssh-clients xrootd-client python3-pip && \
    python3 -m pip install -U uv && \
    uv pip install --exclude-newer "14 days" --no-cache-dir -U --system --only-binary :all: requests && \
    dnf clean all --enablerepo=\*

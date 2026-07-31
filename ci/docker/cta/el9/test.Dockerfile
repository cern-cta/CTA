# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y createrepo epel-release almalinux-release-devel postgresql-server && \
    dnf clean all --enablerepo=\*

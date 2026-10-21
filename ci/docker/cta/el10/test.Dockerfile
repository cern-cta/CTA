# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest

RUN dnf install -y createrepo_c epel-release dnf-plugins-core postgresql-server && \
    dnf config-manager --set-enabled crb && \
    dnf clean all --enablerepo=\*

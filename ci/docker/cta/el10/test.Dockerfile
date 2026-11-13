# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# CI installs the packages under test and configures PostgreSQL as root.
FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2

RUN dnf install -y createrepo_c epel-release dnf-plugins-core postgresql-server util-linux valgrind && \
    dnf config-manager --set-enabled crb && \
    dnf clean all --enablerepo=\*

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest@sha256:e92a563cb0755d351faaa535916fcb0d96b41aed90d86deb038b5ee757c6fd00

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y createrepo epel-release almalinux-release-devel postgresql-server util-linux valgrind && \
    dnf clean all --enablerepo=\*

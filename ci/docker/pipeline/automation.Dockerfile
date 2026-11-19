# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2

ARG UV_VERSION=0.13.9

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y epel-release dnf-plugins-core && \
    dnf install -y git curl jq python3 python3-pip bat && \
    python3 -m pip install --only-binary :all: --no-cache-dir "uv==${UV_VERSION}" && \
    dnf remove -y --setopt=clean_requirements_on_remove=False python3-pip && \
    dnf clean all --enablerepo=\*

# Compatibility checks select 3.9; newer tooling can explicitly select 3.12.
RUN uv python install 3.9 3.12

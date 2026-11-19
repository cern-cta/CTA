# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y epel-release dnf-plugins-core && \
    dnf config-manager --set-enabled crb && \
    dnf install -y git curl jq python3 krb5-workstation rsync openssh-clients xrootd-client && \
    dnf clean all --enablerepo=\*

# Verify the client tools without contacting a repository or obtaining credentials.
RUN command -v kinit && command -v xrdfs && command -v xrdcp && \
    dnf repoquery --help >/dev/null

# Select build Python through PATH; system tools retain /usr/bin/python3.
COPY --from=ghcr.io/astral-sh/uv:0.13.9@sha256:d3b601f0c7b118bc1753febf7c41cc80981df678445e0254284eecf84db79a7d /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/cta/python
ENV UV_PYTHON_BIN_DIR=/opt/cta/bin
ENV PATH="/opt/cta/bin:${PATH}"
COPY .python-version /opt/cta/.python-version
RUN uv python install --default "$(cat /opt/cta/.python-version)"

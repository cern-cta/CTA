# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2 AS cppcheck-build

ARG CPPCHECK_VERSION=2.20.0
# CI uses built-in checks, not custom PCRE1 regex rules (unavailable on EL10).
# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y git gcc-c++ make python3 which && \
    dnf clean all --enablerepo=\* && \
    git clone --depth=1 --branch "${CPPCHECK_VERSION}" https://github.com/danmar/cppcheck /tmp/cppcheck && \
    make -C /tmp/cppcheck install MATCHCOMPILER=yes FILESDIR=/usr/share/cppcheck \
        CXXFLAGS="-O2 -DNDEBUG -Wall -Wno-sign-compare -Wno-unused-function" -j 4

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y epel-release && \
    dnf install -y git curl jq patch python3 bat ShellCheck libstdc++ && \
    dnf clean all --enablerepo=\*

COPY --from=cppcheck-build /usr/bin/cppcheck /usr/bin/cppcheck
COPY --from=cppcheck-build /usr/share/cppcheck /usr/share/cppcheck

# Put the pinned Python first on PATH without replacing /usr/bin/python3, which OS tools use.
COPY --from=ghcr.io/astral-sh/uv:0.13.9@sha256:d3b601f0c7b118bc1753febf7c41cc80981df678445e0254284eecf84db79a7d /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/cta/python
ENV UV_PYTHON_BIN_DIR=/opt/cta/bin
ENV PATH="/opt/cta/bin:${PATH}"
COPY .python-version /opt/cta/.python-version
RUN uv python install --default "$(cat /opt/cta/.python-version)"

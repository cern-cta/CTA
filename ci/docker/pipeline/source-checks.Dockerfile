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

ARG UV_VERSION=0.13.9

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y epel-release && \
    dnf install -y git curl jq patch python3 python3-pip bat ShellCheck libstdc++ && \
    python3 -m pip install --only-binary :all: --no-cache-dir "uv==${UV_VERSION}" && \
    dnf remove -y --setopt=clean_requirements_on_remove=False python3-pip && \
    dnf clean all --enablerepo=\*

COPY --from=cppcheck-build /usr/bin/cppcheck /usr/bin/cppcheck
COPY --from=cppcheck-build /usr/share/cppcheck /usr/share/cppcheck

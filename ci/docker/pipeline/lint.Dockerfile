# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG CPPCHECK_VERSION=2.20.0

RUN dnf install -y epel-release && \
    dnf install -y git git-clang-format patch python3 python3-pip wget which \
        podman bat shellcheck yamllint g++ pcre-devel make && \
    python3 -m pip install -U uv && \
    uv pip install --exclude-newer "14 days" --no-cache-dir -U --system --only-binary :all: \
        cppcheck_codequality jsonschema black ruff detect-secrets pyright[nodejs] && \
    dnf clean all --enablerepo=\*

# TODO: split this lint image into dedicated images not relying on alma9
# We don't need it and it will just be painful during the next migration
RUN git clone --depth=1 --branch "${CPPCHECK_VERSION}" https://github.com/danmar/cppcheck && \
    cd cppcheck && \
    make install MATCHCOMPILER=yes FILESDIR=/usr/share/cppcheck HAVE_RULES=yes \
        CXXFLAGS="-O2 -DNDEBUG -Wall -Wno-sign-compare -Wno-unused-function" -j $(nproc) && \
    cd .. && rm -rf cppcheck

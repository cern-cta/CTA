# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG CPPCHECK_VERSION=2.20.0
ARG CARGO_DENY_VERSION="=0.20.2"
ARG CARGO_MACHETE_VERSION="=0.9.2"
ARG CARGO_NEXTEST_VERSION="=0.9.145"
ARG CARGO_SONAR_VERSION="=1.6.0"

RUN dnf install -y epel-release && \
    dnf install -y git git-clang-format patch python3 python3-pip wget which \
        podman bat shellcheck yamllint g++ pcre-devel make protobuf-compiler && \
    python3 -m pip install -U uv && \
    uv pip install --exclude-newer "14 days" --no-cache-dir -U --system --only-binary :all: \
        cppcheck_codequality jsonschema black ruff detect-secrets pyright[nodejs] && \
    dnf clean all --enablerepo=\* && \
    # Eventually we should split this lint image into dedicated images not relying on alma9
    # We don't need it and it will just be painful during the next migration
    git clone --depth=1 --branch "${CPPCHECK_VERSION}" https://github.com/danmar/cppcheck && \
    cd cppcheck && \
    make install MATCHCOMPILER=yes FILESDIR=/usr/share/cppcheck HAVE_RULES=yes \
        CXXFLAGS="-O2 -DNDEBUG -Wall -Wno-sign-compare -Wno-unused-function" -j $(nproc) && \
    cd .. && rm -rf cppcheck

# Install rustup and nightly toolchain
RUN curl -O https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --default-toolchain nightly && \
    rm rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

# Add clippy (linting)
RUN rustup component add clippy

# Add other Rust code checking tools
RUN cargo install cargo-deny@${CARGO_DENY_VERSION} \
                  cargo-machete@${CARGO_MACHETE_VERSION} \
                  cargo-nextest@${CARGO_NEXTEST_VERSION} \
                  cargo-sonar@${CARGO_SONAR_VERSION}

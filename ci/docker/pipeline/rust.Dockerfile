# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest

ARG UV_VERSION=0.13.9

ARG CARGO_DENY_VERSION="=0.20.2"
ARG CARGO_MACHETE_VERSION="=0.9.2"
ARG CARGO_NEXTEST_VERSION="=0.9.145"
ARG CARGO_SONAR_VERSION="=1.6.0"
ARG CARGO_LLVM_COV_VERSION="=0.9.1"

RUN dnf install -y epel-release dnf-plugins-core && \
    dnf config-manager --set-enabled crb && \
    dnf install -y git curl jq python3 python3-pip bat gcc gcc-c++ make \
        openssl-devel pkgconf-pkg-config protobuf-compiler && \
    python3 -m pip install --only-binary :all: --no-cache-dir "uv==${UV_VERSION}" && \
    dnf remove -y --setopt=clean_requirements_on_remove=False python3-pip && \
    dnf clean all --enablerepo=\*

# Match repository components explicitly; coverage additionally needs llvm-tools-preview.
RUN curl -O https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --default-toolchain nightly --profile minimal \
        --component clippy,rustfmt,rust-docs,llvm-tools-preview && \
    rm rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

# Add other Rust code checking tools
RUN cargo install cargo-deny@${CARGO_DENY_VERSION} \
                  cargo-llvm-cov@${CARGO_LLVM_COV_VERSION} \
                  cargo-machete@${CARGO_MACHETE_VERSION} \
                  cargo-nextest@${CARGO_NEXTEST_VERSION} \
                  cargo-sonar@${CARGO_SONAR_VERSION} && \
    rm -rf /root/.cargo/registry /root/.cargo/git /tmp/cargo-install*

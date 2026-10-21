# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Dockerfile used for building CTA rpms

FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest

ARG CARGO_CYCLONEDX_VERSION="=0.5.9"
ARG SCCACHE_VERSION="=0.18.0"

RUN dnf install -y epel-release dnf-plugins-core git python3-dnf-plugin-versionlock && \
    dnf config-manager --set-enabled crb && \
    dnf install -y gcc gcc-c++ cmake rpm-build make ninja-build \
        systemd-devel clang-tools-extra corrosion && \
    dnf clean all --enablerepo=\*

# Install rustup and nightly toolchain
RUN curl -O https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --default-toolchain nightly && \
    rm rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

# Add cargo-cyclonedx, so that we can generate SBOMs
RUN cargo install cargo-cyclonedx@${CARGO_CYCLONEDX_VERSION}

# For speeding up Cargo builds
RUN cargo install sccache@${SCCACHE_VERSION} --locked --no-default-features

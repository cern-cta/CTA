# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Dockerfile used for building CTA rpms

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG CARGO_CYCLONEDX_VERSION="=0.5.9"
ARG SCCACHE_VERSION="=0.18.0"

# GCC Toolset ships the TSan linker library separately from the compiler.
RUN dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock && \
    dnf install -y gcc-toolset-14-gcc-c++ gcc-toolset-14-libtsan-devel cmake3 rpm-build dnf-utils make ninja-build \
        systemd-devel clang-tools-extra corrosion && \
    dnf clean all --enablerepo=\*

# Select GCC 14 for CMake and tools that invoke the compiler directly.
ENV PATH="/opt/rh/gcc-toolset-14/root/usr/bin:${PATH}"
ENV CC="/opt/rh/gcc-toolset-14/root/usr/bin/gcc"
ENV CXX="/opt/rh/gcc-toolset-14/root/usr/bin/g++"

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

# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Dockerfile used for building CTA rpms

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

ARG CARGO_CYCLONEDX_VERSION="=0.5.9"

RUN dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock && \
    dnf install -y gcc gcc-c++ cmake3 rpm-build dnf-utils make ninja-build \
        ccache systemd-devel clang-tools-extra libasan corrosion && \
    dnf clean all --enablerepo=\*

# Install rustup and nightly toolchain
RUN curl -O https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --default-toolchain nightly && \
    rm rustup-init

ENV PATH="/root/.cargo/bin:${PATH}"

# Add cargo-cyclonedx, so that we can generate SBOMs
RUN cargo install cargo-cyclonedx@${CARGO_CYCLONEDX_VERSION}

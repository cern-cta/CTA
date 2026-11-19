# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Dockerfile used for building CTA rpms

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest@sha256:e92a563cb0755d351faaa535916fcb0d96b41aed90d86deb038b5ee757c6fd00

ARG CARGO_CYCLONEDX_VERSION="=0.5.9"
ARG SCCACHE_VERSION="=0.18.0"

# GCC Toolset ships sanitizer linker libraries separately from the compiler.
# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
RUN dnf install -y epel-release almalinux-release-devel git python3-dnf-plugin-versionlock && \
    dnf install -y gcc-toolset-14-gcc-c++ \
      gcc-toolset-14-libasan-devel gcc-toolset-14-libubsan-devel gcc-toolset-14-libtsan-devel \
      cmake3 rpm-build dnf-utils make ninja-build \
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

# Keep the tool installs separate so changing sccache preserves the cargo-cyclonedx layer.
# hadolint ignore=DL3059
RUN cargo install sccache@${SCCACHE_VERSION} --locked --no-default-features

# Put the pinned Python first on PATH without replacing /usr/bin/python3, which OS tools use.
COPY --from=ghcr.io/astral-sh/uv:0.13.9@sha256:d3b601f0c7b118bc1753febf7c41cc80981df678445e0254284eecf84db79a7d /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/cta/python
ENV UV_PYTHON_BIN_DIR=/opt/cta/bin
ENV PATH="/opt/cta/bin:${PATH}"
COPY .python-version /opt/cta/.python-version
RUN uv python install --default "$(cat /opt/cta/.python-version)"

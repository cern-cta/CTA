# SPDX-FileCopyrightText: 2025 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Dockerfile used for building CTA rpms

# CI installs source-package build dependencies at runtime, which requires root.
FROM gitlab-registry.cern.ch/linuxsupport/alma10-base:latest@sha256:7002f355b8da9e81c9118bf247a2a02722c04e8ecb4c95a360d4be030463b3c2

# Build helpers must support the distribution Python; newer Python tooling belongs in the Python CI image.
ARG CARGO_CYCLONEDX_VERSION="=0.5.9"
ARG SCCACHE_VERSION="=0.18.0"

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3041
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

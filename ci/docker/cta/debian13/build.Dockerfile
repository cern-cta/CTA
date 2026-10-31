# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Build from the repository root: podman build -f ci/docker/cta/debian13/build.Dockerfile .
# CI installs source-package build dependencies at runtime, which requires root.
FROM debian:13

ARG CTA_DEPENDENCY_REPOSITORY=https://cta-public-repo.web.cern.ch/testing/cta-6/debian
ARG CARGO_CYCLONEDX_VERSION="=0.5.9"
ARG SCCACHE_VERSION="=0.18.0"

# Base build tools belong in the image; build-essential also supplies dpkg-dev.
RUN test "$(dpkg --print-architecture)" = amd64 && \
    apt-get update && apt-get install -y --no-install-recommends \
      build-essential fakeroot ca-certificates python3 curl && \
    rm -rf /var/lib/apt/lists/*

# This repository is intentionally unsigned until repository signing is deployed.
RUN printf 'deb [arch=amd64 trusted=yes] %s trixie main\n' "$CTA_DEPENDENCY_REPOSITORY" \
    > /etc/apt/sources.list.d/cta-dependencies.list

# Backported debhelper restores stop-on-removal handling when --no-start is used.
RUN printf 'deb [signed-by=/usr/share/keyrings/debian-archive-keyring.gpg] https://deb.debian.org/debian trixie-backports main\n' \
      > /etc/apt/sources.list.d/debian-backports.list && \
    apt-get update && apt-get install -y --no-install-recommends -t trixie-backports debhelper && \
    rm -rf /var/lib/apt/lists/*

COPY project.json /opt/cta/project.json
COPY ci/project-json/generate_build_requires.py /opt/cta/ci/project-json/generate_build_requires.py
RUN apt-get update && apt-get satisfy -y --no-install-recommends \
      "$(python3 /opt/cta/ci/project-json/generate_build_requires.py --platform debian13)" && \
    rm -rf /var/lib/apt/lists/*

# Use the same nightly Rust channel and pinned cargo-cyclonedx/sccache versions as EL9.
RUN curl --fail --location --proto '=https' --proto-redir '=https' \
      https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init \
      -o /tmp/rustup-init && chmod +x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --default-toolchain nightly && rm /tmp/rustup-init
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install cargo-cyclonedx@${CARGO_CYCLONEDX_VERSION} && \
    cargo install sccache@${SCCACHE_VERSION} --locked --no-default-features

# PostgreSQL server tools are needed by the scheduler's unit-test fixture.
ENV PATH="/usr/lib/postgresql/17/bin:${PATH}"

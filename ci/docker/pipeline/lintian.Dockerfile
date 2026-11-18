# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends lintian && \
    rm -rf /var/lib/apt/lists/*

# Package inspection needs no privileges; provide a writable home for lintian's cache.
RUN useradd --create-home --uid 1000 lintian
USER lintian
WORKDIR /home/lintian

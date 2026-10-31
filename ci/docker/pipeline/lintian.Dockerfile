# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM debian:13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends lintian && \
    rm -rf /var/lib/apt/lists/*

# Package inspection needs no privileges; provide a writable home for lintian's cache.
RUN useradd --create-home --uid 1000 lintian
USER lintian
WORKDIR /home/lintian

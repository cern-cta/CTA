# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM debian:13-slim

# Tool updates are adopted through the versioned pipeline-image refresh.
RUN apt-get update && \
    apt-get install -y --no-install-recommends lintian && \
    rm -rf /var/lib/apt/lists/*

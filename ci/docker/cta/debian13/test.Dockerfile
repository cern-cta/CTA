# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# CI installs the packages under test and configures PostgreSQL as root.
FROM debian:13

ARG CTA_DEPENDENCY_REPOSITORY=https://cta-public-repo.web.cern.ch/testing/cta-6/debian

# CTA packages supply their runtime and test dependencies when installed by the test job.
RUN test "$(dpkg --print-architecture)" = amd64 && \
    apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates postgresql && \
    rm -rf /var/lib/apt/lists/*

# Match the build image's unsigned vendored dependency repository.
RUN printf 'deb [arch=amd64 trusted=yes] %s trixie main\n' "$CTA_DEPENDENCY_REPOSITORY" \
    > /etc/apt/sources.list.d/cta-dependencies.list

# PostgreSQL server tools are needed by the scheduler's unit-test fixture.
ENV PATH="/usr/lib/postgresql/17/bin:${PATH}"

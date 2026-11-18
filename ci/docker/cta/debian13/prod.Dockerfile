# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Base-image digests are updated by Renovate; OS packages receive fixes on rebuild.
# CTA dependency constraints come from the binary packages built from project.json.
# Package inputs and APT caches are mounted, never copied into runtime layers.
FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS repo-builder

# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends dpkg-dev apt-utils && \
    rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3022
COPY --from=package_context . /packages
WORKDIR /packages
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN dpkg-scanpackages --multiversion . /dev/null > Packages && \
    dpkg-scanpackages --multiversion --type ddeb . /dev/null >> Packages && \
    apt-ftparchive -o APT::FTPArchive::Release::Origin=CTA-local release . > Release && \
    find . -type f \( -name '*.deb' -o -name '*.ddeb' \) -exec sha256sum '{}' + | sort > .package-hash

FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS grpc-health-probe
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    curl -fL --proto '=https' --proto-redir '=https' \
      https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.59/grpc_health_probe-linux-amd64 \
      -o /grpc-health-probe && \
    echo "b74ea9e3fe0e4fb19008a77422b6475e29141b30833a6f82c18941bf6eb6a136  /grpc-health-probe" > /tmp/grpc-health-probe.sha256 && \
    sha256sum -c /tmp/grpc-health-probe.sha256 && \
    rm /tmp/grpc-health-probe.sha256 && \
    chmod 0755 /grpc-health-probe && \
    rm -rf /var/lib/apt/lists/*

# hadolint ignore=DL3007
FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest@sha256:e92a563cb0755d351faaa535916fcb0d96b41aed90d86deb038b5ee757c6fd00 AS cern-ca

FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS base
ARG CTA_DEPENDENCY_REPOSITORY=https://cta-public-repo.web.cern.ch/testing/cta-6/debian
ARG SUPPRESS_BUILD_SERVICE_STDOUT=false
ARG ENABLE_ORACLE_SUPPORT=false
# Accepted for the shared build interface; this YUM repository option does not apply to Debian.
ARG ENABLE_INTERNAL_REPOS=false

COPY build-service.sh /usr/local/bin/build-service.sh
COPY --from=cern-ca /etc/ssl/certs/CERN-bundle.pem /tmp/CERN-bundle.pem

# Keep the standard package database and copyright files. Skip optional manuals and translations.
# The cache mounts are locked because parallel builds may reuse them.
# hadolint ignore=DL3008
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-base \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-base \
    test "$(dpkg --print-architecture)" = amd64 && \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    printf 'Binary::apt::APT::Keep-Downloaded-Packages "true";\n' > /etc/apt/apt.conf.d/keep-cache && \
    printf 'path-exclude=/usr/share/man/*\npath-exclude=/usr/share/info/*\npath-exclude=/usr/share/locale/*\n' \
      > /etc/dpkg/dpkg.cfg.d/cta-container && \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates passwd tar jq sudo procps && \
    groupadd -g 1000 cta && useradd -m -u 1000 -g cta cta && \
    install -d -o cta -g cta -m 0750 /run/cta && \
    printf 'cta ALL=(ALL) NOPASSWD: ALL\n' > /etc/sudoers.d/cta && chmod 0440 /etc/sudoers.d/cta && \
    awk '/-----BEGIN CERTIFICATE-----/ {n++} n {print > ("/usr/local/share/ca-certificates/CERN-" n ".crt")}' \
      /tmp/CERN-bundle.pem && \
    update-ca-certificates && rm /tmp/CERN-bundle.pem && \
    chmod +x /usr/local/bin/build-service.sh && \
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d && \
    printf 'deb [trusted=yes] file:/mnt/packages ./\n' > /etc/apt/sources.list.d/cta-local.list && \
    printf 'Package: *\nPin: release o=CTA-local\nPin-Priority: 1001\n' > /etc/apt/preferences.d/cta-local && \
    # Match the build/test images until dependency-repository signing is deployed.
    printf 'deb [arch=amd64 trusted=yes] %s trixie main\n' "$CTA_DEPENDENCY_REPOSITORY" \
      > /etc/apt/sources.list.d/cta-dependencies.list

# Mounted input changes must invalidate installation even when package versions are unchanged.
# hadolint ignore=DL3022
COPY --from=repo-builder /packages/.package-hash /tmp/cta-package-hash

FROM base AS cta-taped
# setcap is needed only while building; the capability remains on the installed binary.
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-taped \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-taped \
    /usr/local/bin/build-service.sh cta-taped cta-eosdf libcap2-bin && \
    setcap cap_sys_rawio=ep /usr/bin/cta-taped && \
    apt-get purge -y libcap2-bin && \
    rm -f /var/log/apt/* /var/log/dpkg.log
USER cta
CMD ["/usr/bin/cta-taped", "--config=/etc/cta/cta-taped.toml", "--config-strict", "--runtime-dir=/run/cta"]

FROM base AS cta-rmcd
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-rmcd \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-rmcd \
    /usr/local/bin/build-service.sh cta-rmcd
USER cta
CMD ["/usr/bin/cta-rmcd", "--config=/etc/cta/cta-rmcd.toml", "--config-strict", "--runtime-dir=/run/cta"]

FROM base AS cta-maintd
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-maintd \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-maintd \
    /usr/local/bin/build-service.sh cta-maintd
USER cta
CMD ["/usr/bin/cta-maintd", "--config=/etc/cta/cta-maintd.toml", "--config-strict", "--runtime-dir=/run/cta"]

FROM base AS cta-admin-api
COPY --from=grpc-health-probe /grpc-health-probe /usr/local/bin/grpc-health-probe
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-admin-api \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-admin-api \
    /usr/local/bin/build-service.sh cta-admin-api
USER cta
CMD ["/usr/bin/cta-admin-api", "--config=/etc/cta/cta-admin-api.toml", "--config-strict", "--runtime-dir=/run/cta"]

FROM base AS cta-workflow-api
COPY --from=grpc-health-probe /grpc-health-probe /usr/local/bin/grpc-health-probe
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-workflow-api \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-workflow-api \
    /usr/local/bin/build-service.sh cta-workflow-api
USER cta
CMD ["/usr/bin/cta-workflow-api", "--config=/etc/cta/cta-workflow-api.toml", "--config-strict", "--runtime-dir=/run/cta"]

FROM base AS cta-tools
COPY --from=grpc-health-probe /grpc-health-probe /usr/local/bin/grpc-health-probe
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-tools \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-tools \
    /usr/local/bin/build-service.sh cta-tools krb5-user bc
ENTRYPOINT ["/bin/bash"]

# Debug packages and tests belong only in this optional, larger image.
FROM base AS cta-debug
RUN --mount=type=bind,from=repo-builder,source=/packages,target=/mnt/packages \
    --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-debian13-debug \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=apt-lists-debian13-debug \
    /usr/local/bin/build-service.sh --debug
ENTRYPOINT ["/bin/bash"]

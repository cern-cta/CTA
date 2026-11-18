# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM docker.io/ruby:3.4-alpine@sha256:68d60620b4458fc58c1e1d04a79ee26b85564fdd5387f3071ae077e2c56ae766

# Danger 9.6.2 supports git gem 6; check startup while building the image.
# OS packages intentionally receive repository fixes on image rebuild.
# hadolint ignore=DL3018
RUN apk add --no-cache git && \
    gem install danger:9.6.2 danger-gitlab:10.0.0 faraday-retry:2.4.0 --no-document && \
    danger --version

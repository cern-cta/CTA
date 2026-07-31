# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM registry.cern.ch/docker.io/ruby:3.4-alpine

RUN apk add --no-cache git && \
    gem install danger danger-gitlab faraday-retry

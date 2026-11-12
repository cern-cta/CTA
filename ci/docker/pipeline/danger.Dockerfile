# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM docker.io/ruby:3.4-alpine

# Danger 9.6.2 supports git gem 6; check startup while building the image.
RUN apk add --no-cache git && \
    gem install danger:9.6.2 danger-gitlab faraday-retry --no-document && \
    danger --version

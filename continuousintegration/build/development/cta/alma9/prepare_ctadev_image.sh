#!/bin/bash -e

# SPDX-FileCopyrightText: 2025 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later


image_tag="dev"
if [ -n "$1" ]; then
    version_arg="--build-arg CTA_VERSION=$1"
    image_tag="${image_tag}-$1"
fi

# Pass to docker the version of CTA to build
podman build --platform linux/amd64 -f Dockerfile -t ctadev:${image_tag} ${version_arg} .

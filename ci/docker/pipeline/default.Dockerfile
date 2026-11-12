# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y epel-release && \
    dnf install -y git python3 python3-pip wget jq podman && \
    python3 -m pip install --only-binary :all: -U uv && \
    uv pip install --exclude-newer "14 days" --no-cache-dir -U --system --only-binary :all: jsonschema && \
    dnf clean all --enablerepo=\*

# Jobs install the docs group from their checkout's shared uv.lock.
# Cache the interpreter in the image; older images can download it via uv sync.
RUN uv python install 3.12

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

FROM gitlab-registry.cern.ch/linuxsupport/alma9-base:latest

RUN dnf install -y epel-release && \
    dnf install -y git python3 python3-pip wget jq podman && \
    python3 -m pip install -U uv && \
    uv pip install --exclude-newer "14 days" --no-cache-dir -U --system --only-binary :all: jsonschema && \
    dnf clean all --enablerepo=\*

# Keep the docs toolchain isolated from other default-image Python utilities.
COPY docs/requirements.txt /opt/docs-requirements.txt
RUN python3 -m venv /opt/docs && \
    uv pip sync --python /opt/docs/bin/python /opt/docs-requirements.txt

#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export UV_PROJECT_ENVIRONMENT="$PWD/docs/.venv"
uv sync --locked --only-group docs
exec "$UV_PROJECT_ENVIRONMENT/bin/python" docs/build.py serve "$@"

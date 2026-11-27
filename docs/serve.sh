#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec uv run --locked python docs/build.py serve "$@"

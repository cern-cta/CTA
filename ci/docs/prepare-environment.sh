#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# Use the same interpreter and locked dependencies as local documentation builds.
cta_docs_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UV_PROJECT_ENVIRONMENT="$cta_docs_root/build/docs/venv" uv sync \
  --project "$cta_docs_root" --locked --only-group docs
export PATH="$cta_docs_root/build/docs/venv/bin:$PATH"
unset cta_docs_root

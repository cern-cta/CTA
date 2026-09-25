#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

export PATH="/opt/docs/bin:$PATH"

# For now just a convenience script so that we don't have to rebuild the pipeline images
if [[ ! -f /opt/docs-requirements.txt ]] || ! cmp --silent docs/requirements.txt /opt/docs-requirements.txt; then
  python3 -m venv --clear build/docs/venv
  uv pip sync --python build/docs/venv/bin/python docs/requirements.txt
  export PATH="$CI_PROJECT_DIR/build/docs/venv/bin:$PATH"
fi

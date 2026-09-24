#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

export PATH="/opt/docs/bin:$PATH"

# Use the baked environment unless this checkout changes the dependency lock.
# A fresh environment prevents packages removed from the lock from leaking in.
if [[ ! -f /opt/docs-requirements.txt ]] || ! cmp --silent docs/requirements.txt /opt/docs-requirements.txt; then
  python3 -m venv --clear build/docs/venv
  uv pip sync --python build/docs/venv/bin/python docs/requirements.txt
  export PATH="$CI_PROJECT_DIR/build/docs/venv/bin:$PATH"
fi

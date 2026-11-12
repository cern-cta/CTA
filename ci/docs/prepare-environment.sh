#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

# Always use the checkout's lockfile, including on older pipeline images.
export UV_PROJECT_ENVIRONMENT="$CI_PROJECT_DIR/docs/.venv"
uv sync --locked --only-group docs --python 3.12
export PATH="$UV_PROJECT_ENVIRONMENT/bin:$PATH"
# Make repository Markdown extensions importable by mike and Zensical.
export PYTHONPATH="$CI_PROJECT_DIR${PYTHONPATH:+:$PYTHONPATH}"

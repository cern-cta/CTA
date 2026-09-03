# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Import setup shared by release-tool and adjacent CI-script tests."""

from __future__ import annotations

import sys
from pathlib import Path

RELEASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RELEASE_DIR / "src"))
sys.path.insert(0, str(RELEASE_DIR))

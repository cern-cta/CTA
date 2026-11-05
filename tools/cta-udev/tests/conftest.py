# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from pathlib import Path

# cta-udev is an RPM-only helper, not a pip package, so its module is not
# importable through site-packages. Make tools/cta-udev/src/ importable for
# the tests in this directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

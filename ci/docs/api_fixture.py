# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""A tiny C++ checkout exercising the production API hooks and configuration."""

from pathlib import Path
import shutil


def prepare_api_fixture(root: Path) -> None:
    """Install production API inputs and hooks into an isolated test checkout."""
    source = Path(__file__).resolve().parents[2]
    (root / "docs").mkdir(exist_ok=True)
    shutil.copytree(source / "docs/api", root / "docs/api")
    shutil.copyfile(source / "docs/hooks.py", root / "docs/api_hooks.py")
    # Exercise the real API hooks without the unrelated cta-admin generator.
    (root / "docs/hooks.py").write_text(
        "from pathlib import Path\n"
        "import runpy\n"
        "hooks = runpy.run_path(str(Path(__file__).with_name('api_hooks.py')))\n"
        "def on_pre_build(config):\n"
        "    hooks['generate_cpp_api'](Path(config.config_file_path).resolve().parent.parent)\n"
        "on_post_build = hooks['on_post_build']\n"
    )
    for directory in (
        "catalogue",
        "common",
        "disk",
        "frontend",
        "lib",
        "maintd",
        "mediachanger",
        "objectstore",
        "rdbms",
        "scheduler",
        "taped",
        "tools",
    ):
        (root / directory).mkdir(exist_ok=True)
    (root / "version.hpp").write_text("// Fixture version\n")
    (root / "common/Widget.hpp").write_text(
        "/** A fixture API. */\nclass Widget { public: void run(); private: int secret; };\n"
    )
    (root / "common/WidgetTest.hpp").write_text("class ExcludedTest {};\n")
    (root / "common/jwt-cpp").mkdir()
    (root / "common/jwt-cpp/Vendor.hpp").write_text("class ExcludedVendor {};\n")

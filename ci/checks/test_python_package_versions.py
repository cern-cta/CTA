# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Check Python package version extraction from CTA release tags."""

from pathlib import Path
import re

from packaging.version import Version
import pytest
import tomli


ROOT = Path(__file__).resolve().parents[2]
PACKAGES = [
    "lib/python/cta-sdk",
    "lib/python/ctaops-sdk",
    "lib/python/ctaops-utils",
    "tools/ctaops-cli",
]


@pytest.mark.parametrize("package", PACKAGES)
@pytest.mark.parametrize("suffix", ["", ".pgsched", ".pgcat", ".pgall"])
def test_release_variants_have_valid_python_versions(package: str, suffix: str) -> None:
    with (ROOT / package / "pyproject.toml").open("rb") as source:
        config = tomli.load(source)
    options = config["tool"]["hatch"]["version"]["raw-options"]
    match = re.fullmatch(options["tag_regex"], f"v5.11.22.0-1{suffix}")
    assert match is not None
    assert Version(match.group("version")) == Version("5.11.22.0.post1")

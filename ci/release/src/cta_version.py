# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Parsing, construction, and ordering for CTA release tags."""

from __future__ import annotations

import re
import sys
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

# Keep tag spelling identical to Python and native package builds.
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib/python"))
from package_version import BASE_RE, TAG_RE

# TODO: Remove historical parsing once maintained release lines have neutral final tags for changelog lookup.
HISTORICAL_RE = re.compile(r"^v([1-9]\d*)\.(\d+)\.(\d+)(?:\.(\d+))?-(\d+)(?:\..+)?$")


class VersionError(RuntimeError):
    """An invalid CTA version or impossible release-version lookup."""


@dataclass(frozen=True)
class CTAVersion:
    """A source release, independent of its platform and backend builds."""

    major: int
    minor: int
    patch: int
    release_candidate: int | None = None
    development: int | None = None
    source_text: str | None = field(default=None, compare=False, repr=False)

    @classmethod
    def parse(cls, text: str, *, require_base: bool = False) -> CTAVersion:
        """Accept only the current tag format for new release operations."""
        match = (BASE_RE if require_base else TAG_RE).fullmatch(text)
        if not match:
            expected = "v<major>.<minor>.<patch>"
            if not require_base:
                expected += "[.devN|.rcN]"
            raise VersionError(f"Invalid CTA version {text!r}; expected {expected}, for example 'v6.12.0'")
        groups = match.groups()
        suffix = None if require_base else groups[3]
        number = None if require_base or groups[4] is None else int(groups[4])
        return cls(
            *map(int, groups[:3]),
            release_candidate=number if suffix == "rc" else None,
            development=number if suffix == "dev" else None,
        )

    @property
    def text(self) -> str:
        """Render the tag, preserving historical spelling for changelog refs."""
        if self.source_text is not None:
            return self.source_text
        text = f"v{self.major}.{self.minor}.{self.patch}"
        if self.release_candidate is not None:
            text += f".rc{self.release_candidate}"
        if self.development is not None:
            text += f".dev{self.development}"
        return text

    @property
    def core(self) -> tuple[int, int, int]:
        """Return the current numeric release fields."""
        return self.major, self.minor, self.patch

    def with_components(self, *, release_candidate: int | None = None, development: int | None = None) -> CTAVersion:
        """Add one positive prerelease counter to an unsuffixed version."""
        if self.release_candidate is not None or self.development is not None:
            raise VersionError("Tag components can only be added to an unsuffixed base version")
        if release_candidate is not None and development is not None:
            raise VersionError("Choose either rcN or devN")
        if any(number is not None and number < 1 for number in (release_candidate, development)):
            raise VersionError("Prerelease numbers must be positive")
        return CTAVersion(*self.core, release_candidate=release_candidate, development=development)


def parsed_versions(tag_names: Iterable[str]) -> list[CTAVersion]:
    """Return current tags, ignoring unrelated and historical forms."""
    versions = []
    for tag_name in tag_names:
        with suppress(VersionError):
            versions.append(CTAVersion.parse(tag_name))
    return versions


def previous_release(release_version: CTAVersion, tag_names: Iterable[str]) -> CTAVersion:
    """Find the previous final release, including transitional historical baselines."""
    target = (*release_version.core, 0, 0)
    candidates: list[tuple[tuple[int, int, int, int, int], CTAVersion]] = []
    for tag_name in tag_names:
        # Neutral final tags are preferred; legacy parsing only supplies older baselines.
        match = BASE_RE.fullmatch(tag_name)
        if match:
            version = CTAVersion.parse(tag_name)
            key = (*version.core, 0, 0)
        else:
            if re.search(r"\.(dev|rc)[0-9]+(?:\.|$)", tag_name):
                continue
            historical = HISTORICAL_RE.fullmatch(tag_name)
            if historical is None:
                continue
            major, minor, patch, old_patch, package = (int(value or 0) for value in historical.groups())
            key = (major, minor, patch, old_patch, package)
            version = CTAVersion(major, minor, patch, source_text=tag_name)
        if key < target:
            candidates.append((key, version))
    if not candidates:
        raise VersionError(f"No previous CTA release tag exists before {release_version.text}")
    return max(candidates, key=lambda item: item[0])[1]


def select_release_candidate(release_version: CTAVersion, tag_names: Iterable[str]) -> int:
    """Select the next unused RC number for this release."""
    return (
        max(
            (
                version.release_candidate or 0
                for version in parsed_versions(tag_names)
                if version.core == release_version.core
            ),
            default=0,
        )
        + 1
    )


def select_development(release_version: CTAVersion, tag_names: Iterable[str]) -> int:
    """Select the next unused dev number for this release."""
    return (
        max(
            (
                version.development or 0
                for version in parsed_versions(tag_names)
                if version.core == release_version.core
            ),
            default=0,
        )
        + 1
    )

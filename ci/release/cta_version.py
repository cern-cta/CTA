# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Parsing and ordering for canonical CTA release versions."""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from collections.abc import Iterable

VERSION_RE = re.compile(r"^v(5)\.(\d+)\.(\d+)\.(\d+)-(\d+)(?:\.([A-Za-z0-9]+))?$")


class VersionError(RuntimeError):
    """An invalid CTA version or impossible release-version lookup."""


@dataclass(frozen=True)
class CTAVersion:
    """A parsed canonical CTA release version."""

    text: str
    xrootd: int
    major: int
    minor: int
    patch: int
    package: int
    suffix: str | None = None

    @classmethod
    def parse(cls, text: str) -> CTAVersion:
        """Parse and validate a canonical CTA version string."""
        match = VERSION_RE.fullmatch(text)
        if not match:
            raise VersionError(
                f"Invalid CTA version {text!r}; expected v5.<major>.<minor>.<patch>-<package>[.<suffix>]"
            )
        groups = match.groups()
        return cls(
            text=text,
            xrootd=int(groups[0]),
            major=int(groups[1]),
            minor=int(groups[2]),
            patch=int(groups[3]),
            package=int(groups[4]),
            suffix=groups[5],
        )

    @property
    def core(self) -> tuple[int, int, int, int, int]:
        """Return the numeric fields used for release ordering."""
        return self.xrootd, self.major, self.minor, self.patch, self.package


def parsed_versions(tags: Iterable[str]) -> list[CTAVersion]:
    """Return all canonical CTA versions found among the supplied tags."""
    versions: list[CTAVersion] = []
    for tag in tags:
        with suppress(VersionError):
            versions.append(CTAVersion.parse(tag))
    return versions


def previous_release(version: CTAVersion, tags: Iterable[str]) -> CTAVersion:
    """Find the greatest numeric release core preceding a target version."""
    candidates = [candidate for candidate in parsed_versions(tags) if candidate.core < version.core]
    if not candidates:
        raise VersionError(f"No previous CTA release tag exists before {version.text}")
    return max(candidates, key=lambda candidate: candidate.core)

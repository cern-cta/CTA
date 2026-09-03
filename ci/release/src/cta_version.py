# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Parsing, construction, and ordering for CTA release tags."""

from __future__ import annotations

import re
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum

BASE_RE = re.compile(r"^v(5)\.(\d+)\.(\d+)\.(\d+)-(\d+)$")
TAG_RE = re.compile(
    r"^v(5)\.(\d+)\.(\d+)\.(\d+)-(\d+)"
    r"(?:\.rc([1-9]\d*))?(?:\.(pgsched|pgcat|pgall))?$"
)
HISTORICAL_RE = re.compile(r"^v(5)\.(\d+)\.(\d+)\.(\d+)-(\d+)(?:\..+)?$")
SUFFIXED_BASE_RE = re.compile(r"^(v5\.\d+\.\d+\.\d+-\d+)(\..+)$")


class VersionError(RuntimeError):
    """An invalid CTA version or impossible release-version lookup."""


class BuildVariant(str, Enum):
    """A supported variation of the release build."""

    PGSCHED = "pgsched"
    PGCAT = "pgcat"
    PGALL = "pgall"

    @property
    def description(self) -> str:
        """Return the additional annotated-tag description for this build."""
        if self is BuildVariant.PGSCHED:
            return "This is a PostgreSQL scheduler release."
        if self is BuildVariant.PGCAT:
            return (
                "This is a PostgreSQL catalogue release without Oracle support; "
                "its Docker images are safe for publication."
            )
        if self is BuildVariant.PGALL:
            return (
                "This is a PostgreSQL scheduler and catalogue release without Oracle support; "
                "its Docker images are safe for publication."
            )

        raise VersionError(f"No tag description is implemented for build variant {self.value!r}")


BUILD_VARIANTS = tuple(BuildVariant)


@dataclass(frozen=True)
class CTAVersion:
    """A validated CTA base version with optional generated tag components."""

    xrootd: int
    major: int
    minor: int
    patch: int
    package: int
    release_candidate: int | None = None
    variant: BuildVariant | None = None
    source_text: str | None = field(default=None, compare=False, repr=False)

    @classmethod
    def parse(cls, text: str, *, require_base: bool = False) -> CTAVersion:
        """Parse a supported CTA tag, optionally requiring an unsuffixed base."""
        match = (BASE_RE if require_base else TAG_RE).fullmatch(text)
        if not match:
            if require_base and (suffixed_match := SUFFIXED_BASE_RE.fullmatch(text)):
                base_version, suffix = suffixed_match.groups()
                suffix_hint = ""
                if tag_match := TAG_RE.fullmatch(text):
                    options = []
                    if tag_match.group(6) is not None:
                        options.append("--release-candidate")
                    if variant := tag_match.group(7):
                        options.extend(("--suffix", variant))
                    suffix_hint = f" Use 'release tag {base_version} {' '.join(options)}' instead."
                raise VersionError(
                    f"Release commands require an unsuffixed base version such as {base_version!r}; "
                    f"do not append {suffix!r} manually.{suffix_hint}"
                )

            expected = "v5.<major>.<minor>.<patch>-<package>"
            example = "v5.12.0.0-1"
            if not require_base:
                expected += "[.rcN][.pgsched|.pgcat|.pgall]"
                example += ".rc1.pgall"
            raise VersionError(f"Invalid CTA version {text!r}; expected {expected}, for example {example!r}")
        groups = match.groups()
        release_candidate = None if require_base else _optional_int(groups[5])
        variant = None if require_base or groups[6] is None else BuildVariant(groups[6])
        return cls(
            xrootd=int(groups[0]),
            major=int(groups[1]),
            minor=int(groups[2]),
            patch=int(groups[3]),
            package=int(groups[4]),
            release_candidate=release_candidate,
            variant=variant,
            source_text=text,
        )

    @property
    def text(self) -> str:
        """Render the canonical tag text."""
        if self.source_text is not None:
            return self.source_text
        text = f"v{self.xrootd}.{self.major}.{self.minor}.{self.patch}-{self.package}"
        if self.release_candidate is not None:
            text += f".rc{self.release_candidate}"
        if self.variant is not None:
            text += f".{self.variant.value}"
        return text

    @property
    def core(self) -> tuple[int, int, int, int, int]:
        """Return the numeric fields used for release ordering."""
        return self.xrootd, self.major, self.minor, self.patch, self.package

    def with_components(
        self,
        *,
        release_candidate: int | None = None,
        variant: BuildVariant | None = None,
    ) -> CTAVersion:
        """Construct a tag for this base with validated RC and variant components."""
        if self.release_candidate is not None or self.variant is not None:
            raise VersionError("Tag components can only be added to an unsuffixed base version")
        if release_candidate is not None and release_candidate < 1:
            raise VersionError("Release candidate numbers must be positive")
        return CTAVersion(*self.core, release_candidate=release_candidate, variant=variant)


def _optional_int(value: str | None) -> int | None:
    """Convert an optional regex group to an integer."""
    return int(value) if value is not None else None


def parse_build_variants(values: Iterable[str]) -> tuple[BuildVariant, ...]:
    """Validate, deduplicate, and canonically order requested build variants."""
    try:
        requested_variants = {BuildVariant(value) for value in values}
    except ValueError as error:
        allowed_variants = ", ".join(variant.value for variant in BUILD_VARIANTS)
        raise VersionError(f"Unsupported release suffix; allowed values: {allowed_variants}") from error

    return tuple(variant for variant in BUILD_VARIANTS if variant in requested_variants)


def parsed_versions(tag_names: Iterable[str]) -> list[CTAVersion]:
    """Return supported modern CTA tags while ignoring historical forms."""
    versions: list[CTAVersion] = []
    for tag_name in tag_names:
        with suppress(VersionError):
            versions.append(CTAVersion.parse(tag_name))
    return versions


def previous_release(release_version: CTAVersion, tag_names: Iterable[str]) -> CTAVersion:
    """Find the greatest historical numeric release core preceding a base version."""
    candidate_versions: list[CTAVersion] = []

    for tag_name in tag_names:
        match = HISTORICAL_RE.fullmatch(tag_name)
        if match is None:
            continue

        groups = match.groups()
        candidate_version = CTAVersion(
            xrootd=int(groups[0]),
            major=int(groups[1]),
            minor=int(groups[2]),
            patch=int(groups[3]),
            package=int(groups[4]),
            source_text=tag_name,
        )
        if candidate_version.core < release_version.core:
            candidate_versions.append(candidate_version)

    if not candidate_versions:
        raise VersionError(f"No previous CTA release tag exists before {release_version.text}")

    return max(candidate_versions, key=lambda candidate: candidate.core)


def select_release_candidate(
    release_version: CTAVersion,
    tag_names: Iterable[str],
) -> int:
    """Select the next RC number after every existing family."""
    families: dict[int, set[BuildVariant | None]] = {}

    # Group existing RC tags for this release by candidate number.
    for tag_version in parsed_versions(tag_names):
        if tag_version.core != release_version.core or tag_version.release_candidate is None:
            continue
        families.setdefault(tag_version.release_candidate, set()).add(tag_version.variant)

    return max(families, default=0) + 1

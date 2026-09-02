# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cta_version import (
    BUILD_VARIANTS,
    CTAVersion,
    BuildVariant,
    VersionError,
    parse_build_variants,
    parsed_versions,
    previous_release,
    select_release_candidate,
)


class VersionTest(unittest.TestCase):
    """Test CTA version parsing, construction, and RC selection."""

    def test_parses_canonical_versions(self) -> None:
        version = CTAVersion.parse("v5.10.11.0-1.rc1")
        assert version.core == (5, 10, 11, 0, 1)
        assert version.release_candidate == 1
        assert version.variant is None

    def test_rejects_invalid_versions(self) -> None:
        for value in ("5.10.11.0-1", "v4.10.11.0-1", "v5.1.2-3", "v5.1.2.3-4.rc-1"):
            with self.subTest(value=value), pytest.raises(VersionError):
                CTAVersion.parse(value)

    def test_suffix_does_not_change_numeric_core(self) -> None:
        assert CTAVersion.parse("v5.10.11.0-1").core == CTAVersion.parse("v5.10.11.0-1.rc1").core

    def test_constructs_supported_tag_components(self) -> None:
        release_version = CTAVersion.parse("v5.10.11.0-1", require_base=True)
        assert release_version.with_components(variant=BuildVariant.PGSCHED).text == "v5.10.11.0-1.pgsched"
        assert (
            release_version.with_components(release_candidate=2, variant=BuildVariant.PGALL).text
            == "v5.10.11.0-1.rc2.pgall"
        )

    def test_rejects_unsupported_or_manual_suffixes(self) -> None:
        for value in (
            "v5.10.11.0-1.test1",
            "v5.10.11.0-1.el9",
            "v5.10.11.0-1.pg1",
            "v5.10.11.0-1.rc0",
            "v5.10.11.0-1.pgsched.rc1",
        ):
            with self.subTest(value=value), pytest.raises(VersionError):
                CTAVersion.parse(value)

    def test_requires_unsuffixed_command_input(self) -> None:
        with pytest.raises(VersionError, match=r"release tag v5\.10\.11\.0-1 --suffix pgsched"):
            CTAVersion.parse("v5.10.11.0-1.pgsched", require_base=True)

    def test_manually_appended_release_candidate_suggests_option(self) -> None:
        with pytest.raises(VersionError, match=r"release tag v5\.10\.11\.0-1 --release-candidate"):
            CTAVersion.parse("v5.10.11.0-1.rc2", require_base=True)

    def test_malformed_version_includes_expected_format_and_example(self) -> None:
        with pytest.raises(VersionError) as error:
            CTAVersion.parse("5.12.0-1", require_base=True)
        assert "v5.<major>.<minor>.<patch>-<package>" in str(error.value)
        assert "v5.12.0.0-1" in str(error.value)

    def test_variants_are_deduplicated_and_canonically_ordered(self) -> None:
        assert parse_build_variants(["pgall", "pgsched", "pgall"]) == (
            BuildVariant.PGSCHED,
            BuildVariant.PGALL,
        )
        assert tuple(BuildVariant) == BUILD_VARIANTS

    def test_rc_selection_handles_complete_and_partial_families(self) -> None:
        release_version = CTAVersion.parse("v5.10.11.0-1", require_base=True)
        assert select_release_candidate(release_version, []) == 1
        assert select_release_candidate(release_version, ["v5.10.11.0-1.rc1"]) == 2
        assert (
            select_release_candidate(
                release_version,
                ["v5.10.11.0-1.rc2", "v5.10.11.0-1.rc2.pgsched"],
            )
            == 3
        )

    def test_explicit_variant_starts_a_new_rc_family(self) -> None:
        release_version = CTAVersion.parse("v5.10.11.0-1", require_base=True)
        assert select_release_candidate(release_version, ["v5.10.11.0-1.rc3"]) == 4
        assert (
            select_release_candidate(
                release_version,
                ["v5.10.11.0-1.rc3.pgsched"],
            )
            == 4
        )

    def test_rc_family_always_allocates_after_existing_base(self) -> None:
        release_version = CTAVersion.parse("v5.10.11.0-1", require_base=True)
        assert (
            select_release_candidate(
                release_version,
                ["v5.10.11.0-1.rc3", "v5.10.11.0-1.rc3.pgsched"],
            )
            == 4
        )

    def test_previous_release_uses_numeric_order(self) -> None:
        tags = ["not-a-release", "v5.9.99.0-1", "v5.10.2.0-1", "v5.10.11.0-1.rc1"]
        result = previous_release(CTAVersion.parse("v5.10.12.0-1"), tags)
        assert result.text == "v5.10.11.0-1.rc1"
        assert len(parsed_versions(tags)) == 3

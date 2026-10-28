# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Source tags stay independent from package build variants."""

import pytest

from cta_version import CTAVersion, VersionError, previous_release, select_release_candidate


@pytest.mark.parametrize("suffix", ["", ".rc1", ".dev2"])
def test_current_tags(suffix: str) -> None:
    version = CTAVersion.parse(f"v6.12.0-1{suffix}")
    assert version.core == (6, 12, 0, 1)
    assert version.text == f"v6.12.0-1{suffix}"


@pytest.mark.parametrize(
    "tag",
    [
        "6.12.0-1",
        "v0.12.0-1",
        "v6.12.0.0-1",
        "v6.12.0-1.pgall",
        "v6.12.0-1.pgsched",
        "v6.12.0-1.pgcat",
        "v6.12.0-1.el9",
        "v6.12.0-1.rc0",
        "v6.12.0-1.dev0",
        "v6.12.0-1.dev1.rc1",
        "v6.12.0-1.rc1.pgall",
    ],
)
def test_rejects_invalid_new_tags(tag: str) -> None:
    with pytest.raises(VersionError):
        CTAVersion.parse(tag)


def test_base_commands_reject_prereleases() -> None:
    with pytest.raises(VersionError):
        CTAVersion.parse("v6.12.0-1.rc1", require_base=True)


def test_prerelease_construction() -> None:
    version = CTAVersion.parse("v6.12.0-1")
    assert version.with_components(development=2).text == "v6.12.0-1.dev2"
    assert version.with_components(release_candidate=3).text == "v6.12.0-1.rc3"
    with pytest.raises(VersionError):
        version.with_components(development=1, release_candidate=1)
    with pytest.raises(VersionError):
        version.with_components(development=0)


def test_rc_numbering_ignores_other_releases_and_dev_tags() -> None:
    version = CTAVersion.parse("v6.12.0-1")
    assert select_release_candidate(version, []) == 1
    assert (
        select_release_candidate(
            version,
            [
                "v6.12.0-1.rc2",
                "v6.12.0-1.dev9",
                "v6.12.1-1.rc10",
                "unrelated",
            ],
        )
        == 3
    )


def test_previous_release_preserves_historical_ref_and_numeric_order() -> None:
    version = CTAVersion.parse("v6.12.0-1")
    tags = ["v6.9.99.0-1", "v6.11.0.1-1.pgall", "v6.11.0.0-2", "unrelated"]
    assert previous_release(version, tags).text == "v6.11.0.1-1.pgall"
    assert previous_release(version, [*tags, "v6.11.1-1"]).text == "v6.11.1-1"
    with pytest.raises(VersionError):
        previous_release(version, ["v6.12.0-1.rc1", "v6.13.0-1"])


def test_previous_release_excludes_prereleases_even_with_historical_suffixes() -> None:
    tags = ["v6.10.0-1", "v6.11.0-1.rc2", "v6.11.0-1.dev1", "v6.11.0.0-1.rc2.pgall"]
    assert previous_release(CTAVersion.parse("v6.12.0-1"), tags).text == "v6.10.0-1"

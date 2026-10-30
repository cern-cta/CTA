# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Source tags stay independent from package build variants."""

import pytest

from release_version import CTAVersion, VersionError, previous_release, select_development, select_release_candidate


@pytest.mark.parametrize("suffix", ["", ".rc1", ".dev2"])
def test_current_tags(suffix: str) -> None:
    version = CTAVersion.parse(f"v6.1.0{suffix}")
    assert version.core == (6, 1, 0)
    assert version.text == f"v6.1.0{suffix}"


@pytest.mark.parametrize(
    "tag",
    [
        "6.1.0",
        "v6.1.0-1",
        "v6.1.0-2.rc1",
        "v0.12.0",
        "v06.1.0",
        "v6.01.0",
        "v6.1.00",
        "v6.01.0.rc1",
        "v6.1.00.dev1",
        "v6.1.0.0-1",
        "v6.1.0.pgall",
        "v6.1.0.pgsched",
        "v6.1.0.pgcat",
        "v6.1.0.el9",
        "v6.1.0.rc0",
        "v6.1.0.dev0",
        "v6.1.0.dev1.rc1",
        "v6.1.0.rc1.pgall",
    ],
)
def test_rejects_invalid_new_tags(tag: str) -> None:
    with pytest.raises(VersionError):
        CTAVersion.parse(tag)


def test_base_commands_reject_prereleases() -> None:
    with pytest.raises(VersionError):
        CTAVersion.parse("v6.1.0.rc1", require_base=True)


@pytest.mark.parametrize("tag", ["v06.1.0", "v6.01.0", "v6.1.00"])
def test_base_commands_reject_leading_zeros(tag: str) -> None:
    with pytest.raises(VersionError):
        CTAVersion.parse(tag, require_base=True)


def test_prerelease_construction() -> None:
    version = CTAVersion.parse("v6.1.0")
    assert version.with_components(development=2).text == "v6.1.0.dev2"
    assert version.with_components(release_candidate=3).text == "v6.1.0.rc3"
    with pytest.raises(VersionError):
        version.with_components(development=1, release_candidate=1)
    with pytest.raises(VersionError):
        version.with_components(development=0)


def test_rc_numbering_ignores_other_releases_and_dev_tags() -> None:
    version = CTAVersion.parse("v6.1.0")
    assert select_release_candidate(version, []) == 1
    assert (
        select_release_candidate(
            version,
            [
                "v6.1.0.rc2",
                "v6.1.0.dev9",
                "v6.1.1.rc10",
                "unrelated",
            ],
        )
        == 3
    )


def test_previous_release_preserves_historical_ref_and_numeric_order() -> None:
    version = CTAVersion.parse("v6.1.0")
    tags = ["v5.9.99.0-1", "v5.11.0.1-1.pgall", "v5.11.0.0-2", "unrelated"]
    assert previous_release(version, tags).text == "v5.11.0.1-1.pgall"
    assert previous_release(version, [*tags, "v5.11.1"]).text == "v5.11.1"
    with pytest.raises(VersionError):
        previous_release(version, ["v6.1.0.rc1", "v6.13.0"])


def test_previous_release_excludes_prereleases_even_with_historical_suffixes() -> None:
    tags = ["v5.10.0", "v5.11.0.rc2", "v5.11.0.dev1", "v5.11.0.0-1.rc2.pgall"]
    assert previous_release(CTAVersion.parse("v6.1.0"), tags).text == "v5.10.0"


def test_next_development_number() -> None:
    version = CTAVersion.parse("v6.1.0")
    assert select_development(version, []) == 1
    assert (
        select_development(
            version,
            [
                "v6.1.0.dev1",
                "v6.1.0.dev3",
                "v6.1.0.rc9",
                "v6.1.1.dev8",
                "v6.1.0.dev7.el9",
                "unrelated",
            ],
        )
        == 4
    )

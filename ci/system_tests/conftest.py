# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path

import pytest

# Ensure pytest knows about the fixtures
pytest_plugins = [
    "system_tests.fixtures.fixtures",
]

_canonical_items_key = pytest.StashKey[list[pytest.Item]]()

#####################################################################################################################
# Commandline options
#####################################################################################################################


def pytest_addoption(parser: pytest.Parser) -> None:
    """Pytest hook that allows for adding custom commandline arguments."""
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption(
        "--connection-config",
        action="store",
        type=Path,
        help="A yaml connection file specifying how to connect to each host",
    )
    parser.addoption(
        "--setup",
        action="store_true",
        help=(
            "Execute setup tests first. Setup includes things such as populating the CTA Catalogue, labeling tapes "
            "and configuring the disk instance."
        ),
    )
    parser.addoption(
        "--teardown",
        action="store_true",
        help=(
            "Execute teardown tests last. Teardown cleans up any leftover from the tests to ensure a clean state "
            "for the next run."
        ),
    )
    parser.addoption(
        "--teardown-first",
        action="store_true",
        help="Run the teardown before starting the tests to ensure a clean start",
    )
    parser.addoption(
        "--verification",
        action="store_true",
        help="Execute verification tests last (e.g. checks for unexpected errors or core dumps)",
    )
    parser.addoption(
        "--test-config",
        type=Path,
        default=Path("config/test_params.toml"),
        help="Path to the config file containing all test parameters",
    )


#####################################################################################################################
# Do some magic to automatically add setup and teardown to the test suite
#####################################################################################################################


def is_test_in_items(test_path: Path, items: list[pytest.Item]) -> bool:
    resolved_test_path = test_path.resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Test suite '{resolved_test_path}' not found!")
    return any(str(resolved_test_path) == str(item.path) for item in items)


def add_test_into_existing_collection(
    test_path: Path,
    items: list[pytest.Item],
    prepend: bool = False,
    allow_duplicate: bool = False,
) -> None:
    resolved_test_path = test_path.resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Required test suite '{resolved_test_path}' not found!")
    # Prevent duplicate registration unless explicitly allowed
    if not is_test_in_items(test_path, items) or allow_duplicate:
        # Import the test to ensure pytest collects its tests
        test_module = pytest.Module.from_parent(items[0].session, path=resolved_test_path)
        tests = [node for node in test_module.collect() if isinstance(node, pytest.Item)]
        index = 0 if prepend else len(items)
        items[index:index] = tests


def add_tests_from_directory(test_directory: Path, items: list[pytest.Item], prepend: bool = False) -> None:
    test_paths = sorted(test_directory.rglob("*_test.py"))
    if not test_paths:
        raise FileNotFoundError(f"No test suites found in '{test_directory.resolve()}'!")

    # Reverse prepended paths so their sorted order is retained in the collection.
    if prepend:
        test_paths.reverse()
    for test_path in test_paths:
        add_test_into_existing_collection(test_path, items, prepend=prepend)


def add_lifecycle_tests(config: pytest.Config, items: list[pytest.Item]) -> None:
    rerun = config.getoption("--lf") or config.getoption("--ff")

    if config.getoption("--setup"):
        add_tests_from_directory(Path("tests/setup"), items, prepend=True)

    if config.getoption("--verification"):
        add_tests_from_directory(Path("tests/verification"), items)

    if config.getoption("--teardown") or config.getoption("--teardown-first"):
        prepend = bool(config.getoption("--teardown-first")) and not rerun
        add_tests_from_directory(Path("tests/teardown"), items, prepend=prepend)


# Let pytest first apply ordinary selectors such as -k to the requested suite,
# then construct and remember the complete CTA lifecycle.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # cta_canonical_items is used to remember the original order of items for --lf and --ff
    if not items:
        config.stash[_canonical_items_key] = []
        return

    add_lifecycle_tests(config, items)
    config.stash[_canonical_items_key] = items[:]


@pytest.hookimpl(tryfirst=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    config = session.config
    last_failed_only = config.getoption("--lf")
    failed_first = config.getoption("--ff")
    # Nothing to do
    if not last_failed_only and not failed_first:
        return

    canonical_items = config.stash[_canonical_items_key]
    # Find the failed tests
    last_failed = config.cache.get("cache/lastfailed", {})
    failed_indexes = [index for index, item in enumerate(canonical_items) if item.nodeid in last_failed]

    # Run either only failed tests or the failed tests first
    if last_failed_only:
        selected_items = [canonical_items[index] for index in failed_indexes]
    elif failed_indexes:
        selected_items = canonical_items[failed_indexes[0] :]
    else:
        selected_items = []

    # Update pytest session to only include the selected items
    selected_nodeids = {item.nodeid for item in selected_items}
    deselected_items = [item for item in session.items if item.nodeid not in selected_nodeids]
    if deselected_items:
        config.hook.pytest_deselected(items=deselected_items)

    session.items[:] = selected_items
    session.testscollected = len(selected_items)

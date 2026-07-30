# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import sys
from pathlib import Path

import pytest


if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

# Ensure pytest knows about the fixtures
pytest_plugins = [
    "system_tests.fixtures.fixtures",
]

#####################################################################################################################
# Commandline options
#####################################################################################################################


def pytest_addoption(parser):
    """Pytest hook that allows for adding custom commandline arguments"""
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption(
        "--connection-config",
        action="store",
        help="A yaml connection file specifying how to connect to each host",
    )
    parser.addoption("--setup", action="store_true", help="Execute setup tests first")
    parser.addoption("--teardown", action="store_true", help="Execute teardown tests last")
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
        type=str,
        default="config/test_params.toml",
        help="Path to the config file containing all test parameters",
    )


def pytest_configure(config):
    """Pytest hook that allows us to augment the config object with additional info after commandline parsing"""
    last_failed = config.getoption("lf")
    failed_first = config.getoption("failedfirst")
    if last_failed and failed_first:
        raise pytest.UsageError("--lf and --ff cannot be used together")

    config.cta_rerun_mode = "lf" if last_failed else "ff" if failed_first else None
    if config.cta_rerun_mode:
        # CTA applies last-failed behavior after constructing the complete
        # setup -> suite -> verification -> teardown flow. Keep pytest's
        # plugin registered so it continues updating the last-failed cache,
        # but disable its collection filtering and ordering.
        last_failed_plugin = config.pluginmanager.get_plugin("lfplugin")
        if last_failed_plugin is not None:
            last_failed_plugin.active = False
        last_failed_collection_wrapper = config.pluginmanager.get_plugin("lfplugin-collwrapper")
        if last_failed_collection_wrapper is not None:
            config.pluginmanager.unregister(last_failed_collection_wrapper)

    config_path: str = config.getoption("--test-config")
    try:
        with open(config_path, "rb") as f:
            config.test_config = tomllib.load(f)
    except FileNotFoundError:
        raise pytest.UsageError(f"--test-config file not found: {config_path}")


#####################################################################################################################
# Do some magic to automatically add setup and teardown to the test suite
#####################################################################################################################


def is_test_in_items(test_path: str, items):
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Test suite '{resolved_test_path}' not found!")
    return any(str(resolved_test_path) == str(item.path) for item in items)


def add_test_into_existing_collection(test_path: str, items, prepend: bool = False, allow_duplicate: bool = False):
    resolved_test_path = Path(test_path).resolve()
    if not resolved_test_path.exists():
        raise FileNotFoundError(f"Required test suite '{resolved_test_path}' not found!")
    # Prevent duplicate registration unless explicitly allowed
    if not is_test_in_items(test_path, items) or allow_duplicate:
        # Import the test to ensure pytest collects its tests
        test_module = pytest.Module.from_parent(items[0].session, path=resolved_test_path)
        tests = test_module.collect()
        index = 0 if prepend else len(items)
        items[index:index] = tests


def add_tests_from_directory(test_directory: Path, items, prepend: bool = False):
    test_paths = sorted(test_directory.rglob("*_test.py"))
    if not test_paths:
        raise FileNotFoundError(f"No test suites found in '{test_directory.resolve()}'!")

    # Reverse prepended paths so their sorted order is retained in the collection.
    if prepend:
        test_paths.reverse()
    for test_path in test_paths:
        add_test_into_existing_collection(str(test_path), items, prepend=prepend)


def add_lifecycle_tests(config, items):
    rerun = config.cta_rerun_mode is not None

    if config.getoption("--setup"):
        add_tests_from_directory(Path("tests/setup"), items, prepend=True)

    if config.getoption("--verification"):
        add_tests_from_directory(Path("tests/verification"), items)

    if config.getoption("--teardown") or config.getoption("--teardown-first"):
        prepend = bool(config.getoption("--teardown-first")) and not rerun
        add_tests_from_directory(Path("tests/teardown"), items, prepend=prepend)


def apply_rerun_mode(config, items):
    if config.cta_rerun_mode is None:
        return

    last_failed = config.cache.get("cache/lastfailed", {})
    failed_indexes = [index for index, item in enumerate(items) if item.nodeid in last_failed]

    if config.cta_rerun_mode == "lf":
        selected_items = [items[index] for index in failed_indexes]
    elif failed_indexes:
        selected_items = items[failed_indexes[0] :]
    else:
        selected_items = []

    selected_nodeids = {item.nodeid for item in selected_items}
    deselected_items = [item for item in items if item.nodeid not in selected_nodeids]
    if deselected_items:
        config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items


# Let pytest first apply ordinary selectors such as -k to the requested suite,
# then construct and filter the complete CTA lifecycle.
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    if not items:
        return

    add_lifecycle_tests(config, items)
    apply_rerun_mode(config, items)

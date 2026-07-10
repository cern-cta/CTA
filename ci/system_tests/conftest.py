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
        "--connection-config", action="store", help="A yaml connection file specifying how to connect to each host"
    )
    parser.addoption("--no-setup", action="store_true", help="Skip the execution of the setup tests")
    parser.addoption("--no-cleanup", action="store_true", help="Skip the execution of the cleanup tests")
    parser.addoption(
        "--cleanup-first", action="store_true", help="Run the cleanup before starting the tests to ensure a clean start"
    )
    # Should probably get a better name
    parser.addoption(
        "--no-modify",
        action="store_true",
        help="Skip all modifications of the pytest Test collections and run only the tests specified",
    )
    parser.addoption(
        "--test-config",
        type=str,
        default="config/test_params.toml",
        help="Path to the config file containing all test parameters",
    )


def pytest_configure(config):
    """Pytest hook that allows us to augment the config object with additional info after commandline parsing"""
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
    if not items:
        raise RuntimeError("No tests found")
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


# Pytest hook that allows us to dynamically modify the set of tests being run
def pytest_collection_modifyitems(config, items):
    if config.getoption("--no-modify"):
        return
    # Always check for errors after the run
    add_test_into_existing_collection("tests/cleanup/error_test.py", items, prepend=False)

    # For now a solution not to run the setup when we do things like --ff, but this should be cleaner in the future
    if not config.getoption("--no-setup") and not config.getoption("--ff") and not config.getoption("--lf"):
        add_test_into_existing_collection("tests/setup/setup_cta_test.py", items, prepend=True)
        add_test_into_existing_collection("tests/setup/setup_eos_test.py", items, prepend=True)
        add_test_into_existing_collection("tests/setup/setup_dcache_test.py", items, prepend=True)

    if not config.getoption("--no-cleanup"):
        # Do the reset before the tests start.
        # Useful when rerunning the tests multiple times on the same instance and it wasn't properly cleaned up
        prepend = bool(config.getoption("--cleanup-first"))
        add_test_into_existing_collection("tests/cleanup/cleanup_cta_test.py", items, prepend=prepend)
        add_test_into_existing_collection("tests/cleanup/cleanup_eos_test.py", items, prepend=prepend)
        add_test_into_existing_collection("tests/cleanup/cleanup_dcache_test.py", items, prepend=prepend)

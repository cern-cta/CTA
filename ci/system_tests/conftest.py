# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path

import pytest
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from urllib3.exceptions import HTTPError

# Ensure pytest knows about the fixtures
pytest_plugins = [
    "system_tests.fixtures.fixtures",
]

_canonical_items_key = pytest.StashKey[list[pytest.Item]]()
_protected_connections_key = pytest.StashKey[frozenset[str]]()

# =========================================================================
#  Commandline options
# =========================================================================


def pytest_addoption(parser: pytest.Parser) -> None:
    """Pytest hook that allows for adding custom commandline arguments."""
    parser.addoption("--namespace", action="store", help="Namespace for tests")
    parser.addoption(
        "--allow-destructive",
        action="store_true",
        help="Allow selected destructive tests against protected connections; does not enable lifecycle steps",
    )
    parser.addoption(
        "--auto-lifecycle",
        action="store_true",
        help="Include verification and, for unprotected instances, setup and teardown (used by cta-dev)",
    )
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


# =========================================================================
#  Do some magic to automatically add setup and teardown to the test suite
# =========================================================================


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

    automatic = config.getoption("--auto-lifecycle")
    automatic_reset = automatic and not config.stash.get(_protected_connections_key, frozenset())

    if config.getoption("--setup") or automatic_reset:
        add_tests_from_directory(Path("tests/setup"), items, prepend=True)

    if config.getoption("--verification") or automatic:
        add_tests_from_directory(Path("tests/verification"), items)

    if config.getoption("--teardown") or config.getoption("--teardown-first") or automatic_reset:
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


# Connection policy is read once; destructive selections are checked before fixtures run.
def get_protected_connections(namespace: str) -> frozenset[str]:
    try:
        config.load_kube_config()
        response = client.CoreV1Api().read_namespaced_config_map(
            "cta-connection-protection", namespace, _request_timeout=30
        )
    except (OSError, ApiException, ConfigException, HTTPError, ValueError) as error:
        raise pytest.UsageError(f"Cannot verify connection protection in {namespace}; refusing system tests") from error
    # The generated client also advertises raw and asynchronous response types.
    if not isinstance(response, client.V1ConfigMap):
        raise pytest.UsageError(f"Invalid connection protection response in {namespace}; refusing system tests")
    policy = response.data
    if not isinstance(policy, dict) or any(
        policy.get(db) not in ("true", "false") for db in ("catalogue", "scheduler")
    ):
        raise pytest.UsageError(f"Invalid connection protection in {namespace}; refusing system tests")
    return frozenset(database for database in ("catalogue", "scheduler") if policy[database] == "true")


def check_destructive_tests(items: list[pytest.Item], protected: frozenset[str], allow_destructive: bool) -> None:
    """Reject the complete selection before any test fixtures can run."""
    blocked = []
    for item in items:
        for marker in item.iter_markers("destructive"):
            targets = frozenset(marker.args) if marker.args else frozenset({"catalogue", "scheduler"})
            if marker.kwargs or not targets <= {"catalogue", "scheduler"}:
                raise pytest.UsageError(f"Invalid destructive marker on {item.nodeid}")
            if targets & protected and not allow_destructive:
                blocked.append(item.nodeid)
                break
    if blocked:
        raise pytest.UsageError(
            "Destructive tests target protected connections; explicitly pass --allow-destructive to run them:\n"
            + "\n".join(blocked)
        )


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    protected = frozenset()
    if not config.option.collectonly:
        namespace = config.getoption("--namespace")
        if namespace:
            protected = get_protected_connections(namespace)
        elif config.getoption("--connection-config"):
            # Remote deployments have no Kubernetes policy; preserve their lifecycle by default.
            protected = frozenset({"catalogue", "scheduler"})
    config.stash[_protected_connections_key] = protected
    if protected and config.getoption("--auto-lifecycle"):
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        if reporter:
            reporter.write_line(
                "Protected connections: skipping automatic setup and teardown; verification remains enabled."
            )


@pytest.hookimpl(tryfirst=True)
def pytest_runtestloop(session: pytest.Session) -> None:
    # All selectors and lifecycle/last-failure ordering have finished; fixtures have not started.
    if not session.config.option.collectonly:
        check_destructive_tests(
            session.items,
            session.config.stash.get(_protected_connections_key, frozenset()),
            session.config.getoption("--allow-destructive"),
        )

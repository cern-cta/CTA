import time
import pytest
import subprocess
from .helpers.setup_tests import setup_tests

@pytest.fixture(scope="module", autouse=True)
def init_env(env):
    print("\n[Setup] Initializing test environment for test-client")
    setup_tests(env)
    yield
    print("\n[Teardown] Inspecting and cleaning up test environment for test-client")

@pytest.mark.order(2)
def test_preflight(env):
    print("|hell9o")
    assert True

@pytest.mark.order(2)
def test_bar(env):
    print("|hell9o")
    assert True

@pytest.mark.order(3)
def test_foo(env):
    for _ in range(0, 2):
        print("Doing sometginf greate")
        time.sleep(1)
    assert False


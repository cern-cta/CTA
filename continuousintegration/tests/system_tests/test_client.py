import time
import pytest


@pytest.mark.order(2)
def test_preflight(env):
    print("preflight")
    assert True

@pytest.mark.order(2)
def test_bar(env):
    print("first proper test")
    assert True

@pytest.mark.order(3)
def test_foo(env):
    for _ in range(0, 2):
        print("Doing something")
        time.sleep(1)
    assert False


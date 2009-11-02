import os, conftest

def test_generic(setup, testSet, testName, fileName):
    setup.runTest(testSet, testName, fileName)

def pytest_generate_tests(metafunc):
    # go through all test cases
    for ts, tn, fn in conftest.listTests('castortests'):
        # add a test case
        metafunc.addcall(funcargs=dict(fileName=fn,testName=tn,testSet=ts),id=tn)


import os, conftest

def test_generic(setup, testSet, testName, fileName):
    setup.runTest(testSet, testName, fileName)

def pytest_generate_tests(metafunc):
    # find out the test directory to be used
    testdir = getattr(metafunc.config.option,'testdir')
    if testdir == None:
        # try to guess
        candidates = filter(lambda x:x.endswith('tests'), os.listdir('.'))
        if len(candidates) == 1:
            testdir = candidates[0]
            metafunc.config.option.testdir = testdir
        elif len(candidates) == 0:
            assert False, 'Could not find test directory. Maybe use --testdir option'
        else:
            assert False, 'Don\'t know which tests to run. Candidates are : ' + ' '.join(candidates)
    # go through all test cases
    for ts, tn, fn in conftest.listTests(testdir, testdir):
        # add a test case
        metafunc.addcall(funcargs=dict(fileName=fn,testName=tn,testSet=ts),id=tn)


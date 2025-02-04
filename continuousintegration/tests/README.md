# Python Tests for CTA

This directory contains the Python system tests for CTA.


## Useful commands:

Run a particular test suite:

```sh
pytest system_tests/test_client.py --namespace dev
```

To produce live output, add the `-s` flag:

```sh
pytest system_tests/test_client.py --namespace dev -s
```

To skip the setup (initialization of the catalogue), add the `--no-setup` flag:

```sh
pytest system_tests/test_client.py --namespace dev -s --no-setup
```

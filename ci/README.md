# Continuous Integration

This directory contains all the files necessary for development and automation workflows, including build scripts, container configurations, orchestration tools, release processes, and utility scripts for the CI pipeline.

* `build/`: Files for building the CTA software
* `checks/`: Collection of scripts that perform validation checks within the CI pipeline
* `danger/`: Configuration for the Danger bot that runs on Merge Requests
* `docs/`: Documentation CI environment, publication, authentication, and publication tests
* `docker/`: Docker files and content to build the docker images
* `orchestration/`: Files to set up a local development cluster
* `project-json/`: Files related to the project.json in the root of the repository
* `release/`: Scripts used by the CI pipeline when doing a new release of the CTA software
* `sbom/`: Utility scripts used during the generating of a Software Bill of Materials for CTA
* `system_tests/`: Pytest based system test for CTA
* `utils/`: Collection of utility scripts
* `build_deploy.sh`: Deprecated: development workflow script that `cta-dev` replaces.
* `ci-debug.sh`: Opens an interactive debug container for investigating core dumps from a CI pipeline.
* `cta-dev.bash-completion`: Script for auto-completion of `cta-dev`. Used during `cta-dev install`.
* `cta-dev.sh`: The main script used for development: builds the project, the corresponding Docker image and deploys a local CTA test instance. See `./cta-dev.sh --help`.
* `ci-download-artifacts.sh`: Downloads and extracts the artifacts of a single GitLab pipeline job.

## Useful links

- [cta-dev reference](../docs/content/dev/guides/tools-and-environment/cta-dev.md): commands and use cases.
- [CI overview](../docs/content/dev/guides/testing/ci/index.md): how GitLab CI fits into development.

## Log Utilities

To get more user-friendly output in the various bash scripts, you can source `utils/log_utils.sh` at the top of the script.


## Release CLI

`release/` contains the release command and CI publication helpers. From the repository root:

```bash
export PATH="$PWD/ci/release:$PATH"
release --help
```

See the [Release Procedure](../docs/content/dev/contributing/maintainers/releases.md) for changelog preparation, tagging, validation, and publication.

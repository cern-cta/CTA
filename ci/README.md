# Continuous Integration

This directory contains all the files necessary for development and automation workflows, including build scripts, container configurations, orchestration tools, release processes, and utility scripts for the CI pipeline.

* `build/`: Files for building the CTA software
* `checks/`: Collection of scripts that perform validation checks within the CI pipeline
* `danger/`: Configuration for the Danger bot that runs on Merge Requests
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

### CTA development versions

`cta-dev` uses one identifier for CTA packages and container images:
`<cta-version>-<cta-version-suffix>`. The base version accepts numbers and dots;
the suffix accepts lowercase letters, numbers, dots, and hyphens. For example,
the defaults `5` and `dev` produce package version and image tag `5-dev`.

Configure these values with `--cta-version` and `--cta-version-suffix`, or copy
`.cta-dev.env.example` to `.cta-dev.env` and set `CTA_DEV_CTA_VERSION` and
`CTA_DEV_CTA_VERSION_SUFFIX`. CMake historically exposes the suffix as
`VCS_VERSION`; the developer and CI interfaces call it the CTA version suffix.

## Useful links

- `cta-dev` docs and use cases: https://cta.docs.cern.ch/latest/dev/getting-started/cta-dev/
- CI overview, including explanations of the GitLab CI: https://cta.docs.cern.ch/latest/dev/ci/overview/

## Log Utilities

To get more user-friendly output in the various bash scripts, you can source `utils/log_utils.sh` at the top of the script.

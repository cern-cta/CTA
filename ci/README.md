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

### CTA development versions

`cta-dev` uses one identifier for CTA packages and container images:
`--cta-version <version>-<suffix>`, which defaults to `6-dev`. The part before the
first hyphen becomes the RPM version and accepts numbers and dots; the part after
it becomes the RPM release and accepts lowercase letters, numbers, dots, and
hyphens. CMake historically exposes the suffix as `VCS_VERSION`. `cta-dev` splits
the identifier and passes the two parts separately to the underlying build
scripts, which is also how the CI invokes them.

The CTA version is also the tag of the container images built from those RPMs, so
`build`, `images`, `up`, `debug`, and `all` take `--cta-version` only.

`deploy` additionally accepts `--cta-image-tag` to deploy images that were built
elsewhere, for example a CI image tag such as `5426528gitf4d8f0eb`. It cannot be
combined with `--cta-version`, which selects a locally built version instead.
Because such a tag normally refers to an image that is not on the local machine,
`--cta-image-tag` also switches the image registry from `localhost` to the CI
registry; override that with `--cta-image-registry`.

Configure the default CTA version with `--cta-version`, or copy
`.cta-dev.env.example` to `.cta-dev.env` and set `CTA_DEV_CTA_VERSION`. The image
tag and registry are command-line options only.

## Useful links

- `cta-dev` docs and use cases: https://cta.docs.cern.ch/latest/dev/guides/tools-and-environment/cta-dev/
- CI overview, including explanations of the GitLab CI: https://cta.docs.cern.ch/latest/dev/guides/testing/ci/

## Log Utilities

To get more user-friendly output in the various bash scripts, you can source `utils/log_utils.sh` at the top of the script.


## Release CLI

`release/` contains the release command and CI publication helpers. From the repository root:

```bash
export PATH="$PWD/ci/release:$PATH"
release --help
```

See the [Release Procedure](../docs/content/dev/contributing/maintainers/releases.md) for changelog preparation, tagging, validation, and publication.

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


## Release CLI

The release workflow is exposed as a Python command in `ci/release`. Add it to your `PATH` for the current shell:

```bash
export PATH="$PWD/ci/release:$PATH"
release --help
```

Every command requires an explicit, unsuffixed release version such as `v5.12.0.0-1`.
Prepare the changelog from `main` with:

```bash
release changelog v5.12.0.0-1
```

The command validates and synchronizes the selected target branch, generates changes since the previous numeric CTA release, and opens the proposed changelog in the editor selected by Git (`GIT_EDITOR`, `core.editor`, `VISUAL`, or `EDITOR`).

Review and merge the printed changelog merge request, then tag the release:

```bash
release tag v5.12.0.0-1
```

The tag command finds the unique merged changelog MR and tags its merge or squash commit after verifying that the commit is reachable from the selected target branch.
It validates release metadata and the commit pipeline, opens Git's editor for a shared annotated-tag description, verifies that the complete selected tag family is absent locally and remotely, and asks for final confirmation.
The tags are pushed atomically.

By default, `changelog`, `tag`, and `status` use `main` as the target branch.
For another release branch, pass the same `--target-branch` to every command:

```bash
release changelog v5.12.0.0-1 --target-branch maintenance
release status v5.12.0.0-1 --target-branch maintenance
release tag v5.12.0.0-1 --target-branch maintenance
```

Use `status` to inspect the release issue, changelog MR, release commit, pipeline, and tags without making changes:

```bash
release status v5.12.0.0-1
```

By default, a final release publishes the base tag and the `pgsched`, `pgcat`, and `pgall` variants.
Use one or more `--suffix` options to publish only selected variants without the base tag:

```bash
release tag v5.12.0.0-1 --suffix pgsched --suffix pgcat
```

Use `--release-candidate` to select the next unused RC number automatically.
The selected RC or final tag family must be completely new; the command fails if any member already exists locally or remotely.

```bash
release tag v5.12.0.0-1 --release-candidate
```

If release metadata is incomplete or the release commit does not have a successful push pipeline, `tag` displays the current state and asks whether to continue.
This approval is separate from the final publication confirmation.
Use `--yes` with `tag` to accept its confirmations in unattended use.

Place the global `--dry-run` option before the command to perform validation and display planned decisions without editing files or making local or remote mutations:

```bash
release --dry-run changelog v5.12.0.0-1
release --dry-run tag v5.12.0.0-1
```

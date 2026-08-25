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

Prepare a release from a clean, synchronized `main` checkout:

```bash
release prepare v5.12.0.0-1
```

The changelog opens in the editor selected by Git (`GIT_EDITOR`, `core.editor`, `VISUAL`, or `EDITOR`).

Review and merge the printed changelog merge request, then tag the release:

```bash
release tag
```

By default, `tag` resolves and tags the latest fetched `origin/main`. Pass any
Git revision—such as a commit SHA, local branch, remote-tracking branch, or
existing tag—with `--ref`:

```bash
release tag v5.12.0.0-1 --ref origin/maintenance
release tag v5.12.0.0-1 --ref 0123456789abcdef
```

When a version is explicit, missing release issues, merge requests, or
changelog entries produce warnings and require confirmation. Without a
version, the command requires an unambiguous merged release MR.

Before creating a new annotated tag, the command opens Git's configured editor
for a short tag description. For the default `origin/main` target, only a
successful push pipeline for the selected commit satisfies the pipeline gate.
On completion, the command prints links to the GitLab tag page and tag
pipeline.

Use `release --dry-run prepare VERSION` or `release --dry-run tag VERSION`
to validate and print planned mutations.

For testing the release command from a dirty feature-branch checkout, add
`--allow-unclean`. This skips the worktree and current-branch checks, but still
resolves the requested tag target explicitly; `prepare` still requires local
`main` to match `origin/main`:

```bash
release --dry-run --allow-unclean prepare VERSION
```

Run the release-tool tests with:

```bash
python3 -m unittest discover -s ci/release/tests -v
```

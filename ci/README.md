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
* `build_deploy.sh`: The main script used for development: builds the project, the corresponding Docker image and deploys a local CTA test instance. See [the docs on Build & Deploy](https://cta.docs.cern.ch/latest/dev/setup/build-deploy/) for more details
* `ci-debug.sh`: Opens an interactive debug container for investigating core dumps from a CI pipeline.
* `cta-dev.bash-completion`: Script for auto-completion of `cta-dev`. Used during `cta-dev install`.
* `cta-dev.sh`: Future replacement of `build_deploy.sh`. See `./cta-dev.sh --help` for more details.

## Common `cta-dev` workflows

Unlike the old `build_deploy.sh` script, `cta-dev` separates the development workflow into independent commands. You can run individual stages as needed, or use the convenience commands `up` (build -> images -> deploy) and `all` (build -> images -> deploy -> test).

### Build only

Compile the CTA RPMs inside the persistent build container:

```bash
cta-dev build
```

Recreate the build container from scratch:

```bash
cta-dev build --reset
```

Run the unit tests after building:

```bash
cta-dev build --unit-tests
```

Use a different scheduler implementation during the build and deployment:

```bash
cta-dev up --scheduler-type pgsched
```

### Build container images

Rebuild the CTA container images from the generated RPMs:

```bash
cta-dev images
```

By default, the provided Docker images are very minimal and don't provide various utilities and debuginfo packages. For this, you can build the debug image:

```bash
# This image can be used in the Kubernetes setup using [kubectl debug](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_debug/).
cta-dev images --enable-debug-image
```

### Deploy

Deploy a fresh development instance:

```bash
# Will tear down the old deployment first
cta-dev deploy
```

Deploy with custom Helm values:

```bash
cta-dev deploy \
  --catalogue-config my-catalogue-values.yaml \
  --scheduler-config my-scheduler-values.yaml \
  --cta-config my-cta-values.yaml
```

Deploy a local telemetry stack alongside CTA:

```bash
cta-dev deploy --local-telemetry
```

### Run the full workflow

Build, create images, and deploy:

```bash
cta-dev up
```

Build, deploy, and immediately run the system tests:

```bash
cta-dev all
```

### Run system tests

`cta-dev test` treats the system tests as one ordered flow:

```text
setup -> selected test suite -> verification -> teardown
```

Setup, verification, and teardown tests are included by default.

Launch the interactive test selector:

```bash
cta-dev test
```

Run a specific test:

```bash
cta-dev test client
```

Run one lifecycle phase on its own:

```bash
cta-dev test setup # Useful for when you want to run some manual commands
cta-dev test verification
cta-dev test teardown
```

Additional arguments after the test suite are forwarded directly to `pytest`:

```bash
cta-dev test client -k test_simple_archive_retrieve # Run a specific test
cta-dev test client --teardown-first # Run the teardown first to wipe any test leftovers
```

Use `--lf` to rerun only the previously failed test:

```bash
cta-dev test client --lf
```

For example, if `test_verify_catalogue` failed during setup, only that test is rerun:

```text
test_verify_catalogue
```

Use `--ff` to resume the flow from the previously failed test:

```bash
cta-dev test client --ff
```

If a test failed in the selected suite, the completed setup and earlier suite tests are skipped:

```text
failed suite test -> remaining suite tests -> verification -> teardown
```

If a verification or teardown test failed, the run resumes within that phase:

```text
failed verification test -> remaining verification tests -> teardown
```

```text
failed teardown test -> remaining teardown tests
```

If there is no previous failure for the selected flow, `--lf` and `--ff` run nothing.

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

## Typical helper functions and their meaning

```sh
# Local script defined function which displays usage information.
usage() {
  echo
  echo "Usage: $0 --parameter <tag> [options] ..."
  exit 1
}

# This function is invoked on fatal failures.
die() {
  echo "$@" >&2
  exit 1
}

# This function displays some error and exits with failure.
die_usage() {
  echo "$@" >&2
  usage
}

# Like die_usage() but with "Error: " prefix in the message.
error_usage() {
  die_usage "Error: $@"
}
```

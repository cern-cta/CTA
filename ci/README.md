# Continuous Integration

This directory contains all the files necessary for development and automation workflows, including build scripts, container configurations, orchestration tools, release processes, and utility scripts for the CI pipeline.

* `build/`: Files for building the CTA software
* `docker/`: Docker files and content to build the docker images
* `checks/`: Collection of scripts that perform validation checks within the CI pipeline
* `orchestration/`: Files to set up a local development cluster and all the tests that can run on this.
* `release/`: Scripts used by the CI pipeline when doing a new release of the CTA software
* `utils/`: Collection of utility scripts
* `build_deploy.sh`: The main script used for development: builds the project, the corresponding Docker image and deploys a local CTA test instance. See [the docs on Build & Deploy](https://cta.docs.cern.ch/latest/dev/setup/build-deploy/) for more details.
* `deploy.sh`: Convenience script for deploying a custom CTA image.

# Typical helper functions and their meaning

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

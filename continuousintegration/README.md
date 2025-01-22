# Continuous Integration

This directory contains all the files necessary for development and automation workflows, including build scripts, container configurations, orchestration tools, release processes, and utility scripts for the CI pipeline.

* `build/`: Files for building the CTA software
* `docker/`: Docker files and content to build the docker images
* `orchestration/`: Files to set up a local development cluster and all the tests that can run on this.
* `release/`: Scripts used by the CI pipeline when doing a new release of the CTA software
* `util/`: Collection of utility scripts
* `validation/`: Collection of scripts that perform validation checks within the CI pipeline
* `build_deploy.sh`: The main script used for development: builds the project, the corresponding Docker image and deploys a local CTA test instance. See [the docs on development commands](https://eoscta.docs.cern.ch/latest/dev/development_commands/) for more details.

# How to update the EOS version used in CI

1. ssh to **aiadm**, then:
```
$ cd /eos/user/c/ctareg/www/cta-ci-repo/
$ cp <path_to_rpms>/eos-*.rpm eos/x86_64/
$ ./update_repos.sh
```
2. In the CTA repository, update the yum version lock list with the new EOS version:
```
/CTA/continuousintegration/docker/alma9/etc/yum/pluginconf.d/versionlock.list
```

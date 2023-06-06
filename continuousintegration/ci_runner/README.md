# Run a CTA test instance from build tree in an independent virtual machine.

For the test instance details, see the orchestration directory.

The scripts in this directory allow to run the continuous integration system tests from a locally built CTA, without RPMs.

The target is to run in a virtual machine, possibly in a disconnected laptop.

## Setting up a fresh virtual machine

### User environment

The vmBootstrap directory contains all the necessary script to go from minimal CC7 instalation to running kubernetes with CTA checked out and compiled.

A full CTA source tree should be cloned or copied in the target system, and scripts should be run from .../CTA/continuousintegration/buildtree_runner/vmBootstrap:

```
cd .../CTA/continuousintegration/buildtree_runner/vmBootstrap
./bootstrapSystem.sh
```

This will create a new user (currently hardcoded to "eric") and prompt for the password. The use will be a sudoer (no password).

### CTA build tree

The user should then login as the user, kinit with a valid CERN.CH token, and then run the next step: bootstrapCTA.sh:
```
kinit user@CERN.CH
cd .../CTA/continuousintegration/buildtree_runner/vmBootstrap
./bootstrapCTA.sh
```

This will check out CTA from git, in install the necessary build RPMs and compile.

### Kubernetes setup

The user should then run the script to setup kubernetes:
```
cd .../CTA/continuousintegration/buildtree_runner/vmBootstrap
./bootstrapKubernetes.sh
```

A reboot is currently required at that point.

## Running the system tests.

### Docker image

The system tests run with a single image with all the non CTA rpms pre-loaded. The image should be generated once for all:

```
cd ~/CTA/continuousintegration/buildtree_runner
./prepareImage.sh
```

This image also contains embedded scripts from the CTA tree. As the build is 2 stage, the embedding of scripts can be run separately, saving the time to install the RPMs.

When updating image content, be careful that some layers can be cached event if the content changed (for example an `ADD` layer may not be updated in the new image...). To make sure everything is clean, you need to remove all cached layers from `docker`:
```
docker rmi $(docker images -a | tail -n+2 | awk '{print $3}')
```

### Preparing the environment (MHVTL, kubernetes volumes...)

MHVTL should then be setup by running:

```
cd ~/CTA/continuousintegration/buildtree_runner
sudo ./recreate_buildtree_running_environment.sh
```

### Preparing the test instance

The test instance can then be created by running (typically):

```
cd ~/CTA/continuousintegration/orchestration
sudo ./create_instance.sh -n ctatest -b /home/eric -B CTA-build -D -O
```

and the tests will run like in the continuous integration environment.


# `ctafrontend` docker image repository
  - This is the repository for CERN Tape Archive frontend docker image.


## Aim

The aim of this project is to provide a CTA frontend through Docker® containers.

## Components

* Dockerfile      - The file describing how to build the Docker image for building CTA, in turn.
* etc/yum.repos.d - directory containing yum repos for installing necessary packages.
* etc/xrootd      - directory containing static configuration files for xrootd 
* run.sh          - The main script to setup runtime environment.

## Setup

In order to be able to use the container, you should have Docker installed on your machine. You can get more information on how to setup Docker [here](https://docs.docker.com/linux/).

The base image used is CERN CentOS 7 (gitlab-registry.cern.ch/linuxsupport/cc7-base).

## Build image

In order to build the image, after making sure that the Docker daemon is running, run from the repository directory:

```bash
docker build . -f continuousintegration/docker/ctafrontend/cc7/Dockerfile -t ctageneric:${image_tag}
```

After the image has finished building successfully, run the following command:

```bash
# Run CTA fronted with prepared object store and catalogue DB. 
# All logs will be passed back to the docker host through /dev/log socket. 
# Shared path must be passed inside to the container.
# A host name have to be used for the container  to run xrootd.
#
# @param objectstore  CTA object store directory path name.
# @param catdb        CTA catalogue DB setup.
#

docker run -h ctasystestf.cern.ch -it -e objectstore="/shared/jobStoreVFS1FlpYW" -e catdb="sqlite:/shared/sqliteDb/db" -v /dev/log:/dev/log -v /opt/cta/docker:/shared ctafrontend-cc7
```


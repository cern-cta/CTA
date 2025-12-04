# Docker

This directory contains for each platform a set of file. Every platform must contain the following files:

- `etc/yum.repos.d-internal/*.repo` for the internal yum/dnf repos
- `etc/yum.repos.d-public/*.repo` for the public yum/dnf repos
- `build.Dockerfile` dockerfile for creating the build image used in `build_deploy.sh`
- `dev-local-rpms.Dockerfile` dockerfile for creating an image based on locally-built RPMs. This image does not come with CTA installed and is meant only for dev environments.
- `dev-remote-rpms.Dockerfile` dockerfile for creating an image based on a tagged CTA version with RPMs available in the remote testing repo. This image does not come with CTA installed and is meant only for dev environments.
- `prod-local-rpms.Dockerfile` dockerfile for creating an image per service based on locally-built RPMs. These images that can be built from this Dockerfile are immutable and are suitable for production.
- `prod-remote-rpms.Dockerfile` dockerfile for creating an image per service based on a tagged CTA version with RPMs available in the remote testing repo. These images that can be built from this Dockerfile are immutable and are suitable for production.

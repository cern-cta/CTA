# Docker

This directory contains for each platform a set of file. Every platform must contain the following files:

- `etc/yum.repos.d-internal/*.repo` for the internal yum/dnf repos
- `etc/yum.repos.d-public/*.repo` for the public yum/dnf repos
- `build.Dockerfile` dockerfile for creating the build image used in `build_deploy.sh`
- `dev-local-rpms.Dockerfile` dockerfile for creating an image based on locally-built RPMs. This image does not come with CTA installed and is meant only for dev environments.
- `dev-remote-rpms.Dockerfile` dockerfile for creating an image based on a tagged CTA version with RPMs available in the remote testing repo. This image does not come with CTA installed and is meant only for dev environments.
- `cta.Dockerfile` dockerfile for creating an image per service based on locally-built RPMs (compiled with the Objectstore scheduler). These images that can be built from this Dockerfile are immutable and are suitable for production.
- `cta-pg.Dockerfile` dockerfile for creating an image per service based on locally-built RPMs (compiled with the Postgres scheduler). These images that can be built from this Dockerfile are immutable and are suitable for production.

Note that this army of Dockerfiles should eventually go away. The goal is to only keep a `build.Dockerfile` for the build container and a `cta.Dockerfile` for building the CTA images.
Both `dev` images will go, as these will be replaced by the immutable images. We no longer need the distinction between local and remote RPMs, because we can just grab the image of a previously published version directly.
Finally, the current `cta.Dockerfile` will be replaced by `cta-pg.Dockerfile` once support for the Objectstore scheduler stops.

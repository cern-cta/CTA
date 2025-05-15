# Docker

This directory contains for each platform a set of file. Every platform must contain the following files:
- `etc/yum.repos.d-internal/*.repo` for the internal yum/dnf repos
- `etc/yum.repos.d-public/*.repo` for the public yum/dnf repos
- `build.Dockerfile` dockerfile for creating the build image used in `build_deploy.sh`
- `local-rpms.Dockerfile` dockerfile for creating an image based on locally-built RPMs
- `remote-rpms.Dockerfile` dockerfile for creating an image based on a tagged CTA version with RPMs available in the remote testing repo

# Docker

This directory contains for each platform a set of file. Every platform must contain the following files:
- `etc/yum.repos.d-internal/*.repo` for the internal yum/dnf repos
- `etc/yum.repos.d-public/*.repo` for the public yum/dnf repos
- `artifacts.Dockerfile` dockerfile for creating an image based on locally-built RPMs
- `build.Dockerfile` dockerfile for creating the build image used in `build_deploy.sh`
- `tag.Dockerfile` dockerfile for creating an image based on a tagged CTA version available in the testing repo

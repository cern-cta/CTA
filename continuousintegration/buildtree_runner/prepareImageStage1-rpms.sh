#!/bin/bash -e

(cd ~/CTA; sudo docker build . -f continuousintegration/docker/buildtree_runner/cc7/stage1-rpms/Dockerfile -t buildtree-runner-stage1)


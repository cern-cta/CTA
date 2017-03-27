#!/bin/bash -e

(cd ~/CTA; sudo docker build . -f continuousintegration/docker/buildtree_runner/cc7/stage2-scripts/Dockerfile -t buildtree-runner)

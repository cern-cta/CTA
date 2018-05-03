#!/bin/bash -e

(cd ~/CTA; sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/buildtree-stage3-scripts/Dockerfile -t buildtree-runner)

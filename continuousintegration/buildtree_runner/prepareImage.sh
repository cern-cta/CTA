#!/bin/bash -e

echo Creating the docker image...
(cd ~/CTA; sudo docker build . -f continuousintegration/docker/buildtree-runner/cc7/Dockerfile -t buildtree-runner)

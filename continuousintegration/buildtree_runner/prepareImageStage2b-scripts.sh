#!/bin/bash -e

(cd ~/CTA; sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/doublebuildtree-stage2b-scripts/Dockerfile -t doublebuildtree-runner)

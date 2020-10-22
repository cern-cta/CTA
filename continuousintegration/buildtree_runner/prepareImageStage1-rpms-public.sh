#!/bin/bash -e

(cd ~/CTA; sudo docker build . -f continuousintegration/docker/ctafrontend/cc7/buildtree-stage1-rpms-public/Dockerfile -t buildtree-runner-stage1)

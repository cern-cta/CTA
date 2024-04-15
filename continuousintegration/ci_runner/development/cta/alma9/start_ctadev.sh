#!/bin/bash -e

export IMAGE_TAG=$1
envsubst < ctadev_env.yaml | kubectl apply -f -

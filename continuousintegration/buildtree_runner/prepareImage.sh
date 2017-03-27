#!/bin/bash -e

echo Creating the docker image...
./prepareImageStage1-rpms.sh
./prepareImageStage2-scripts.sh

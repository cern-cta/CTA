#!/bin/bash -e

echo Creating the docker image...
./prepareImageStage1-rpms.sh
./prepareImageStage2-eos.sh
./prepareImageStage3-scripts.sh
./prepareImageStage2b-scripts.sh
#!/bin/bash -e

PUBLIC=true
if [[ $1 == "cern" ]]; then
  PUBLIC=false
  echo Going to install from cern repositories
fi

echo Creating the docker image...
if [[ "$PUBLIC" == false ]] ; then
  ./prepareImageStage1-rpms.sh
else
  ./prepareImageStage1-rpms-public.sh
fi
./prepareImageStage2-eos.sh
./prepareImageStage3-scripts.sh
./prepareImageStage2b-scripts.sh

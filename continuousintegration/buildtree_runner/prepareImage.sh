#!/bin/bash -e

PUBLIC=false
if [[ $1 == "public" ]]; then
  PUBLIC=true
  echo Going to install from public repositories
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

#!/bin/bash

source /opt/rh/devtoolset-8/enable
mkdir -p /eos/build
cd /eos/build
cmake3 ..
if [ "$1" == "rpm" ]; then
  make rpm
else 
  make -j7
fi

exit 0

#!/bin/bash

source /opt/rh/devtoolset-8/enable
mkdir -p /eos/build
cd /eos/build
cmake3 ..
make rpm
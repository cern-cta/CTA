#!/bin/sh

LD_LIBRARY_PATH=`find ../../.. -name '*.so*' | sed 's|/[^/]*$||' | sort | uniq`
LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed 's/ /:/g'`
PARAMS=$@

sudo -u stage sh -c "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH; `pwd`/tpcp $PARAMS"

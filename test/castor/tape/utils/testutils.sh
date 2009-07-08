#!/bin/sh

LD_LIBRARY_PATH=`find ../../../.. -name '*.so*' | sed 's|/[^/]*$||' | sort | uniq`
LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed 's/ /:/g'`
PARAMS=$@
CMD="export LD_LIBRARY_PATH=$LD_LIBRARY_PATH; `pwd`/testutils $PARAMS"

if test $USER = root; then
  sudo -u stage sh -c "$CMD"
else
  sh -c "$CMD"
fi

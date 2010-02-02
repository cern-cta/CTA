#!/bin/sh

if test "x$CASTOR_SRC" = x; then
  echo "Error: The environment variable CASTOR_SRC is not set"
  echo
  echo "CASTOR_SRC should be the full path to the CASTOR source code, e.g."
  echo
  echo "    export CASTOR_SRC=/usr/local/src/CASTOR_SVN_CO/trunk"
  echo
  exit -1
fi

LD_LIBRARY_PATH=`find $CASTOR_SRC -name '*.so*' | sed 's|/[^/]*$||' | sort | uniq`
LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed 's/ /:/g'`
PARAMS=$@
CMD="export LD_LIBRARY_PATH=$LD_LIBRARY_PATH; $CASTOR_SRC/test/castor/tape/python/testpython $PARAMS"

if test $USER = root; then
  sudo -u stage sh -c "$CMD"
else
  sh -c "$CMD"
fi

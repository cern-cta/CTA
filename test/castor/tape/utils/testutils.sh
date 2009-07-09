#!/bin/sh

if test "x$CASTOR_CVS" = x; then
  echo "Error: The environment variable CASTOR_CVS is not set"
  echo
  echo "CASTOR_CVS should be the full path to the CASTOR source code up to and"
  echo "including CASTOR2, e.g."
  echo
  echo "    export CASTOR_CVS=/usr/local/src/CASTOR2"
  echo
  exit -1
fi

LD_LIBRARY_PATH=`find $CASTOR_CVS -name '*.so*' | sed 's|/[^/]*$||' | sort | uniq`
LD_LIBRARY_PATH=`echo $LD_LIBRARY_PATH | sed 's/ /:/g'`
PARAMS=$@
CMD="export LD_LIBRARY_PATH=$LD_LIBRARY_PATH; $CASTOR_CVS/test/castor/tape/utils/testutils $PARAMS"

if test $USER = root; then
  sudo -u stage sh -c "$CMD"
else
  sh -c "$CMD"
fi

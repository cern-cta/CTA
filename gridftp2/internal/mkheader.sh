#! /bin/sh

FLAVOR=gcc32dbg
ARCH=`uname -m`
if [ "${ARCH}" = "x86_64" ]
then
   FLAVOR=gcc64dbg
fi


COMM="$GLOBUS_LOCATION/bin/globus-makefile-header -flavor=${FLAVOR} globus_gridftp_server > makefile_header"
echo ${COMM}
echo ${COMM} | sh

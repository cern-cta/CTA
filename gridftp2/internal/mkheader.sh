#! /bin/sh

COMM="$GLOBUS_LOCATION/bin/globus-makefile-header -flavor=gcc32dbg globus_gridftp_server > makefile_header"
echo ${COMM}
echo ${COMM} | sh

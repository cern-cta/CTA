#!/bin/sh
#
# test regression between two versions of rfiod on the same remote host.
# The production rfiod may run on the usual rfio port while the new
# version runs on a different port (e.g. 8888). This test script only
# tests the differences between two version. It doesn't test whether
# rfiod responds correctly or not. The test therefore assumes that the
# production server is certified to function OK.
#
if  [ $# -lt 2 ]
then 
     echo "Usage $0 remhost newport"
     exit 2
fi
remhost=$1
newport=$2
rm ${remhost}.out
rm ${remhost}_${newport}.out
unset RFIO_PORT
./regression_tests ${remhost}:/tmp/rfio_reg 262144 ${remhost}.out
grep -- '->' ${remhost}.out | cut -c11- > ${remhost}.filtered
export RFIO_PORT=$newport
./regression_tests ${remhost}:/tmp/rfio_reg 262144 ${remhost}_${newport}.out
grep -- '->' ${remhost}_${newport}.out | cut -c11- > ${remhost}_${newport}.filtered
diff ${remhost}_${newport}.filtered ${remhost}.filtered

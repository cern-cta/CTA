#!/bin/bash

if [ -z "$2" ]
then
	echo "Specify server and authentication mechanisms"
	echo "for example:"
	echo "./run_client.sh lxs5012 GSI KRB5"
	exit
fi

SERVER=$1
shift

export LD_LIBRARY_PATH=./shlib:../../shlib:/opt/globus/lib:$LD_LIBRARY_PATH
export CSEC_MECH=$*
export CSEC_TRACE=3 
export X509_USER_CERT=/afs/cern.ch/user/l/lopic3/.globus/hostcert.pem
export X509_USER_KEY=/afs/cern.ch/user/l/lopic3/.globus/hostkey.pem

echo $LD_LIBRARY_PATH
./Csec_client $SERVER

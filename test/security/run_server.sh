#!/bin/bash

if [ `whoami` != 'root' ]
then
	echo "Run this as root!"
	exit
fi

if [ -z "$1" ]
then
	echo "Specify authentication mechanisms"
	echo "for example:"
	echo "./run_server.sh GSI KRB5"
	exit
fi

#cd /usr/lib
#ln -s libshift.so libshift.so.__MAJOR_CASTOR_VERSION__
#ln -s libshift.so.__MAJOR_CASTOR_VERSION__ libshift.so.__MAJOR_CASTOR_VERSION__.__MINOR_CASTOR_VERSION__
#cd -

export LD_LIBRARY_PATH=./shlib:../../shlib:/opt/globus/lib:$LD_LIBRARY_PATH
export CSEC_AUTHMECH=$*
export CSEC_TRACE=3 
export X509_USER_CERT=/etc/grid-security/hostcert.pem
export X509_USER_KEY=/etc/grid-security/hostkey.pem

echo $LD_LIBRARY_PATH
./Csec_server

#!/bin/bash

# $Id: makeshlib.sh,v 1.8 2008/11/26 14:51:58 sponcec3 Exp $

if (( $# < 3 )); then
	echo This script is internally executed by make to build or install the shared libraries.
	exit 1
fi

if [ "x$MAJOR_CASTOR_VERSION" = "x" ]; then MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__ ; fi
if [ "x$MINOR_CASTOR_VERSION" = "x" ]; then MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__ ; fi

target=$2

if [ "$1" = "install" ]; then

echo  installing $target.$MAJOR_CASTOR_VERSION

install $target.$MAJOR_CASTOR_VERSION.$MINOR_CASTOR_VERSION $3.$MAJOR_CASTOR_VERSION.$MINOR_CASTOR_VERSION
rm -f $3.$MAJOR_CASTOR_VERSION
ln -s $target.$MAJOR_CASTOR_VERSION.$MINOR_CASTOR_VERSION $3.$MAJOR_CASTOR_VERSION
rm -f $3
ln -s $target.$MAJOR_CASTOR_VERSION $3

else
	echo This script does not support command \'"$1"\'
	exit 1
fi


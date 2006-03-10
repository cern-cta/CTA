#!/bin/bash

# $Id: makeshlib.sh,v 1.5 2006/03/10 13:50:38 itglp Exp $

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

cmd=$1
echo  building $target.$MAJOR_CASTOR_VERSION

# this nasty trick allows getting all the args from the 3rd one on
shift
shift
cd tmp ; \
$cmd -Wl,-soname,$target.$MAJOR_CASTOR_VERSION -o ../$target.$MAJOR_CASTOR_VERSION.$MINOR_CASTOR_VERSION *.o $@ ; \
cd ..

rm -rf tmp
rm -f $target.$MAJOR_CASTOR_VERSION
ln -s $target.$MAJOR_CASTOR_VERSION.$MINOR_CASTOR_VERSION $target.$MAJOR_CASTOR_VERSION
rm -f $target
ln -s $target.$MAJOR_CASTOR_VERSION $target

fi


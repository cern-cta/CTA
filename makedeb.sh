#!/bin/sh

# $Id: makedeb.sh,v 1.14 2005/05/02 10:13:40 jdurand Exp $

#
## Do tarball
#
. ./maketar.sh
thisstatus=$?
if [ $thisstatus != 0 ]; then
    exit $thisstatus
fi

echo "### INFO ### Creating debs"

#
## Build the package
#
fakeroot dpkg-buildpackage
thisstatus=$?

exit $thisstatus

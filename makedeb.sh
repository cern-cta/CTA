#!/bin/sh

# $Id: makedeb.sh,v 1.13 2005/05/02 08:11:33 jdurand Exp $

#
## Ask for a changelog
#
cvs update -Ad debian/changelog
fakeroot dch --newversion ${a}.${b}.${c}-${d}
cvs commit  -m "Version ${a}.${b}.${c}-${d}" debian/changelog

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

#!/bin/zsh

# $Id: makerpm.zsh,v 1.2 2005/01/20 10:53:16 jdurand Exp $

set -vx

if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MAJOR_CASTOR_VERSION environment variable"
  exit 1
fi
if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MINOR_CASTOR_VERSION environment variable"
  exit 1
fi

cd /tmp
foreach this (castor-*_${MAJOR_CASTOR_VERSION}-${MINOR_CASTOR_VERSION}_*deb)
  dir=`echo $this | sed 's/\-[0-9].*//g' | sed 's/_/\-/g'`
  fakeroot rm -rf $dir
  fakeroot alien --to-rpm --generate --keep-version $this
  cd $dir
  spec=`find . -name "*.spec"`
  mv $spec $spec.orig
  grep -v "%dir" $spec.orig > $spec
  fakeroot rpmbuild -bb $spec
  cd ..
end

#!/bin/zsh

# $Id: makerpm.zsh,v 1.3 2005/01/20 11:05:44 jdurand Exp $

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
  initscript=`grep init.d $spec | awk '{print $NF}' | sed 's/"//g'`
  mv $spec $spec.orig
  grep -v "%dir" $spec.orig > $spec
  if [ "x${initscript}" != "x" ]; then
    init=`basename $initscript`
  echo "
%post
chkconfig --add $init
if [ \"\$1\" = \"1\" ]; then
  # This is a first install
  service $init condrestart
fi

%preun
if [ \"\$1\" = \"0\" ]; then
  # This is a removal
  service $init stop
  chkconfig --del $init
fi

%postun
if [ \"\$1\" = \"1\" ]; then
  # This is an install
  service $init condrestart
fi
" >> $spec
  fi
  fakeroot rpmbuild -bb $spec
  cd ..
end

exit 0


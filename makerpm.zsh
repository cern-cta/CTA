#!/bin/zsh

# $Id: makerpm.zsh,v 1.5 2005/01/23 15:24:03 jdurand Exp $

if [ "x${MAJOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MAJOR_CASTOR_VERSION environment variable"
  exit 1
fi
if [ "x${MINOR_CASTOR_VERSION}" = "x" ]; then
  echo "No MINOR_CASTOR_VERSION environment variable"
  exit 1
fi

#
## We expect ${MAJOR_CASTOR_VERSION} in the form a.b
#
echo ${MAJOR_CASTOR_VERSION} | egrep -q '^[0-9]+\.[0-9]+$'
if [ $? -ne 0 ]; then
  echo "MAJOR_CASTOR_VERSION (${MAJOR_CASTOR_VERSION}) should be in the form a.b, example: 2.0"
  exit 1
fi

#
## We expect ${MINOR_CASTOR_VERSION} in the form c.d
#
echo ${MINOR_CASTOR_VERSION} | egrep -q '^[0-9]+\.[0-9]+$'
if [ $? -ne 0 ]; then
  echo "MINOR_CASTOR_VERSION (${MINOR_CASTOR_VERSION}) should be in the form c.d, example: 99.1"
  exit 1
fi

a=`echo ${MAJOR_CASTOR_VERSION} | sed 's/\..*//g'`
b=`echo ${MAJOR_CASTOR_VERSION} | sed 's/.*\.//g'`
c=`echo ${MINOR_CASTOR_VERSION} | sed 's/\..*//g'`
d=`echo ${MINOR_CASTOR_VERSION} | sed 's/.*\.//g'`

pushd ..
foreach this (castor-*_${a}.${b}.${c}-${d}_*deb)
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

popd

exit 0


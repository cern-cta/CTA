#!/bin/sh

# set build to YES for client components
for this in BuildCommon BuildNameServerClient BuildRfioClient BuildStageClient BuildStageLibrary BuildVdqmClient BuildVolumeMgrClient; do
    perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tYES/g" config/site.def
done

# define castor version - todo get it from changelog
MAJOR_CASTOR_VERSION=2.1
MINOR_CASTOR_VERSION=8.0
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION

# build software
echo ============== Start of makeclient ###################
make -f Makefile.ini Makefiles
which makedepend >& /dev/null
[ $? -eq 0 ] && make depend
make -j $((`grep processor /proc/cpuinfo | wc -l`*2))
make
echo ================ End of make #################################

# install
export RPM_BUILD_ROOT=`pwd`/etics-tmp
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}/usr/bin
if [ arch == 'x86_64' ]; then
  mkdir -p ${RPM_BUILD_ROOT}/usr/lib64
else
  mkdir -p ${RPM_BUILD_ROOT}/usr/lib
fi
mkdir -p ${RPM_BUILD_ROOT}/usr/include/shift
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man1
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man3
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man4

echo =================== End of makeclient ########################

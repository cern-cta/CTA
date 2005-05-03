# $Id$
#
## Generic macros
#  --------------
%define name castor
%define version __A__.__B__.__C__
%define release __D__
#
## General settings
#  ----------------
Summary: Cern Advanced mass STORage
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
URL: http://cern.ch/castor
License: http://cern.ch/castor/DIST/CONDITIONS
Group: Application/Castor
#
## Dependencies
#  ------------
# Note: needs an lsf-master repackaging
#BuildRequires: stk-ssi-devel, lsf-master
BuildRoot: %{_builddir}/%{name}-%{version}-root
#
## RPM specific definitions
#  ------------------------
# Should unpackaged files in a build root terminate a build?
%define __check_files %{nil}
# Don't build debuginfo packages
%define debug_package %{nil}
# Prevents binaries stripping
%define __spec_install_post %{nil}
# Falls back to original find_provides and find_requires
%define _use_internal_dependency_generator 0

%description
The CASTOR Project stands for CERN Advanced STORage Manager, and its goal is to handle LHC data in a fully distributed environment.
%prep
%setup -q
%build
make -f Makefile.ini Makefiles
# In case makedepend is not in the PATH
PATH=${PATH}:/usr/X11R6/bin
export PATH
which makedepend >& /dev/null
[ $? -eq 0 ] && make depend
if [ -z "${ORACLE_HOME}" ]; then
  [ -r /etc/sysconfig/castor ] && . /etc/sysconfig/castor
fi
if [ -z "${ORACLE_HOME}" ]; then
  echo "### ERROR ### ORACLE_HOME is not defined"
  exit 1
fi
make
%install
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}/usr/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/lib/rtcopy
mkdir -p ${RPM_BUILD_ROOT}/usr/include/shift
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man1
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man3
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man4
mkdir -p ${RPM_BUILD_ROOT}/etc/castor
mkdir -p ${RPM_BUILD_ROOT}/etc/sysconfig
mkdir -p ${RPM_BUILD_ROOT}/etc/init.d
mkdir -p ${RPM_BUILD_ROOT}/etc/logrotate.d
mkdir -p ${RPM_BUILD_ROOT}/usr/local/lsf/lib
mkdir -p ${RPM_BUILD_ROOT}/usr/local/lsf/etc
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.d
mkdir -p ${RPM_BUILD_ROOT}/etc/cron.hourly
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.daily
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.weekly
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.monthly
make install DESTDIR=${RPM_BUILD_ROOT}
make exportman DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
(cd clips; ../imake/imake -I../config DESTDIR=${RPM_BUILD_ROOT}; make install DESTDIR=${RPM_BUILD_ROOT})
for i in debian/*CONFIG; do
    install -o root -g bin -m 600 $i ${RPM_BUILD_ROOT}/etc/castor/`basename $i`
done
for i in debian/*.init; do
    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/init.d/`basename $i | sed 's/\.init//g'`
done
for i in debian/*.logrotate; do
    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/logrotate.d/`basename $i | sed 's/\.logrotate//g'`
done
#for i in debian/*.cron.d; do
#    install -o root -g bin -m 644 ${i} ${RPM_BUILD_ROOT}/etc/cron.d/`basename $i | sed 's/\.cron\.d//g'`
#done
for i in debian/*.cron.hourly; do
    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.hourly/`basename $i | sed 's/\.cron\.hourly//g'`
done
#for i in debian/*.cron.daily; do
#    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.daily/`basename $i | sed 's/\.cron\.daily//g'`
#done
#for i in debian/*.cron.weekly; do
#    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.weekly/`basename $i | sed 's/\.cron\.weekly//g'`
#done
#for i in debian/*.cron.monthly; do
#    install -o root -g bin -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.monthly/`basename $i | sed 's/\.cron\.monthly//g'`
#done
for i in `find . -name "*.sysconfig"`; do
    install -o root -g bin -m 600 ${i} ${RPM_BUILD_ROOT}/etc/sysconfig/`basename $i | sed 's/\.sysconfig//g'`
done

#
## The following will be filled dynamically with the rule: make rpm
#

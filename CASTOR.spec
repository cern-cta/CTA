# $Id$
#
## Generic macros
#  --------------
%define name castor
%define version __A__.__B__.__C__
%define release __D__
#
## Conditional packaging a-la-RPM
#  ------------------------------
%{expand:%define has_oracle_home %(if [ -z $ORACLE_HOME ]; then echo 0; else echo 1; fi)}
%if ! %has_oracle_home
%{expand:%define has_oracle_home %([ -r /etc/sysconfig/castor ] && . /etc/sysconfig/castor; if [ -z $ORACLE_HOME ]; then echo 0; else echo 1; fi)}
%endif
%if ! %has_oracle_home
%define has_oracle 0
%else
%{expand:%define has_oracle %(if [ ! -r $ORACLE_HOME/lib/libclntsh.so ]; then echo 0; else echo 1; fi)}
%endif
%{expand:%define has_stk_ssi %(rpm -q stk-ssi-devel >&/dev/null && rpm -q stk-ssi >&/dev/null; if [ $? -ne 0 ]; then echo 0; else echo 1; fi)}
%{expand:%define has_lsf %(if [ -d /usr/local/lsf/lib -a -d /usr/local/lsf/include ]; then echo 1; else echo 0; fi)}

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
# In case makedepend is not in the PATH
PATH=${PATH}:/usr/X11R6/bin
export PATH
# define castor version (modified by maketar.sh to put the exact version)
MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__
MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION
# Don't compile ORACLE related packages if ORACLE is not present
%if ! %has_oracle
echo "### Warning, no ORACLE environment"
echo "The following packages will NOT be built:"
echo "castor-devel-oracle, castor-dlf-server, castor-lib-oracle, castor-lsf-plugin, castor-ns-server, castor-rh-server, castor-rtcopy-clientserver, castor-rtcopy-mighunter, castor-stager-server, castor-upv-server, castor-vmgr-server"
for this in BuildCupvDaemon BuildDlfDaemon BuildNameServerDaemon BuildRHCpp BuildRtcpclientd BuildSchedPlugin BuildVolumeMgrDaemon UseOracle UseScheduler BuildOraCpp BuildStageDaemon BuildVDQMCpp BuildDbTools; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%else
if [ -z "${ORACLE_HOME}" ]; then
  [ -r /etc/sysconfig/castor ] && . /etc/sysconfig/castor
fi
%endif
%if ! %has_lsf
echo "### Warning, no LSF environment"
echo "The following packages will NOT be built:"
echo "castor-lsf-plugin, castor-job"
for this in BuildSchedPlugin BuildJob BuildRmMaster; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
%if ! %has_stk_ssi
echo "### Warning, no STK environment"
echo "The following packages will NOT be built:"
echo "castor-tape-server"
for this in BuildTapeDaemon; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
find . -type f -exec touch {} \;
make -f Makefile.ini Makefiles
which makedepend >& /dev/null
[ $? -eq 0 ] && make depend
make
%install
# define castor version (modified by maketar.sh to put the exact version)
MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__
MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}/usr/bin
mkdir -p ${RPM_BUILD_ROOT}/usr/sbin
%ifarch x86_64
mkdir -p ${RPM_BUILD_ROOT}/usr/lib64/rtcopy
%else
mkdir -p ${RPM_BUILD_ROOT}/usr/lib/rtcopy
%endif
mkdir -p ${RPM_BUILD_ROOT}/usr/lib/perl/CASTOR
mkdir -p ${RPM_BUILD_ROOT}/usr/include/shift
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man1
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man3
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man4
mkdir -p ${RPM_BUILD_ROOT}/etc/castor
mkdir -p ${RPM_BUILD_ROOT}/etc/castor/expert
mkdir -p ${RPM_BUILD_ROOT}/etc/sysconfig
mkdir -p ${RPM_BUILD_ROOT}/etc/init.d
mkdir -p ${RPM_BUILD_ROOT}/etc/logrotate.d
%ifarch x86_64
mkdir -p ${RPM_BUILD_ROOT}/usr/local/lsf/lib64
%else
mkdir -p ${RPM_BUILD_ROOT}/usr/local/lsf/lib
%endif
mkdir -p ${RPM_BUILD_ROOT}/usr/local/lsf/etc
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.d
# Note: Only castor-job subpackage have a cron job
mkdir -p ${RPM_BUILD_ROOT}/etc/cron.hourly
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.daily
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.weekly
#mkdir -p ${RPM_BUILD_ROOT}/etc/cron.monthly
mkdir -p ${RPM_BUILD_ROOT}/var/spool/cleaning
mkdir -p ${RPM_BUILD_ROOT}/var/spool/dlf
mkdir -p ${RPM_BUILD_ROOT}/var/spool/expert
mkdir -p ${RPM_BUILD_ROOT}/var/spool/gc
mkdir -p ${RPM_BUILD_ROOT}/var/spool/job
mkdir -p ${RPM_BUILD_ROOT}/var/spool/monitor
mkdir -p ${RPM_BUILD_ROOT}/var/spool/msg
mkdir -p ${RPM_BUILD_ROOT}/var/spool/ns
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rfio
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rhserver
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rmc
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rmmaster
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rmnode
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rtcopy
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rtcpclientd
mkdir -p ${RPM_BUILD_ROOT}/var/spool/sacct
mkdir -p ${RPM_BUILD_ROOT}/var/spool/stager
mkdir -p ${RPM_BUILD_ROOT}/var/spool/tape
mkdir -p ${RPM_BUILD_ROOT}/var/spool/upv
mkdir -p ${RPM_BUILD_ROOT}/var/spool/vdqm
mkdir -p ${RPM_BUILD_ROOT}/var/spool/scheduler
mkdir -p ${RPM_BUILD_ROOT}/var/spool/vmgr
make install DESTDIR=${RPM_BUILD_ROOT}
make exportman DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
# Install policies
(cd clips; ../imake/imake -I../config DESTDIR=${RPM_BUILD_ROOT}; make install DESTDIR=${RPM_BUILD_ROOT})
# Install example configuration files
for i in debian/*CONFIG; do
    install -m 640 ${i} ${RPM_BUILD_ROOT}/etc/castor/`basename ${i}`.example
done
# Install the debian+redhat init script
for i in debian/*.init; do
    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/init.d/`basename ${i} | sed 's/\.init//g'`
done
# Install the debian+redhat package generic scriptlets
install -m 755 debian/castor-service.postinst ${RPM_BUILD_ROOT}/usr/sbin/castor-service.postinst
install -m 755 debian/castor-service.postrm ${RPM_BUILD_ROOT}/usr/sbin/castor-service.postrm
install -m 755 debian/castor-service.prerm ${RPM_BUILD_ROOT}/usr/sbin/castor-service.prerm
# Install the sample castor.conf
install -m 644 debian/castor.conf ${RPM_BUILD_ROOT}/etc/castor/castor.conf.example
# Install the recommended castor.conf for all nodes but tape servers
install -m 644 debian/castor-allservicesbuttapeservers.conf ${RPM_BUILD_ROOT}/etc/castor/castor.conf
for i in debian/*.logrotate; do
    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/logrotate.d/`basename ${i} | sed 's/\.logrotate//g'`
done
#for i in debian/*.cron.d; do
#    install -m 644 ${i} ${RPM_BUILD_ROOT}/etc/cron.d/`basename ${i} | sed 's/\.cron\.d//g'`
#done
for i in debian/*.cron.hourly; do
    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.hourly/`basename ${i} | sed 's/\.cron\.hourly//g'`
done
#for i in debian/*.cron.daily; do
#    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.daily/`basename ${i} | sed 's/\.cron\.daily//g'`
#done
#for i in debian/*.cron.weekly; do
#    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.weekly/`basename ${i} | sed 's/\.cron\.weekly//g'`
#done
#for i in debian/*.cron.monthly; do
#    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/cron.monthly/`basename ${i} | sed 's/\.cron\.monthly//g'`
#done
for i in `find . -name "*.sysconfig"`; do
    install -m 644 ${i} ${RPM_BUILD_ROOT}/etc/sysconfig/`basename ${i} | sed 's/\.sysconfig//g'`.example
done

#
## Hardcoded package name CASTOR-client for RPM transation from castor1 to castor2
%package -n CASTOR-client
Summary: Cern Advanced mass STORage
Group: Application/Castor
Requires: castor-commands,castor-stager-clientold,castor-lib,castor-doc,castor-rtcopy-client,castor-upv-client,castor-rtcopy-messages,castor-ns-client,castor-stager-client,castor-vdqm-client,castor-rfio-client,castor-vmgr-client,castor-devel,castor-tape-client
%description -n CASTOR-client
castor (Cern Advanced STORage system)  Meta package for CASTOR-client from castor1 transition to castor2
%files -n CASTOR-client

#
## The following will be filled dynamically with the rule: make rpm, or make tar
#

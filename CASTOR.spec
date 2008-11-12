# Generic macros
#---------------
%define name castor
%define version __A__.__B__.__C__
%define release __D__

# Conditional packaging a-la-RPM
#-------------------------------
%ifarch x86_64
%define LIB lib64
%define FLAVOR gcc64dbg
%else
%define LIB lib
%define FLAVOR gcc32dbg
%endif

# ORACLE definitions
#-------------------
%{expand:%define has_oracle_home %(if [ -z $ORACLE_HOME ]; then echo 0; else echo 1; fi)}
%if ! %has_oracle_home
%{expand:%define has_oracle_home %([ -r /etc/sysconfig/castor ] && . /etc/sysconfig/castor; if [ -z $ORACLE_HOME ]; then echo 0; else echo 1; fi)}
%endif
%if ! %has_oracle_home
%define has_oracle 0
%else
%{expand:%define has_oracle %(if [ ! -r $ORACLE_HOME/lib/libclntsh.so ]; then echo 0; else echo 1; fi)}
%endif

# STK definitions
#----------------
%{expand:%define compiling_nostk %(if [ -z $CASTOR_NOSTK ]; then echo 0; else echo 1; fi)}
%{expand:%define has_stk_ssi %(rpm -q stk-ssi-devel >&/dev/null && rpm -q stk-ssi >&/dev/null; if [ $? -ne 0 ]; then echo 0; else echo 1; fi)}

# LSF definitions
#----------------
%{expand:%define has_lsf %(if [ -e /usr/%{LIB}/liblsf.so -a -d /usr/include/lsf ]; then echo 1; else echo 0; fi)}

# Globus defintions
#------------------
%{expand:%define has_globus_location %(if [ -z $GLOBUS_LOCATION ]; then echo 0; else echo 1; fi)}
%if ! %has_globus_location
# Try some defaults
%{expand:%define has_globus %(if [ ! -r /opt/globus/lib/libglobus_ftp_control_%{FLAVOR}.so ]; then echo 0; else echo 1; fi)}
%else
%{expand:%define has_globus %(if [ ! -r $GLOBUS_LOCATION/lib/libglobus_ftp_control_%{FLAVOR}.so ]; then echo 0; else echo 1; fi)}
%endif
%if %has_globus
%{expand:%define has_globus %(if [ ! -d $GLOBUS_LOCATION/include -a ! -d /opt/globus/include ]; then echo 0; else echo 1; fi)}
%endif

# Python definitions
#-------------------
%define _python_lib %(python -c "from distutils import sysconfig; print sysconfig.get_python_lib()")

# General settings
#-----------------
Summary: Cern Advanced mass STORage
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
URL: http://cern.ch/castor
License: http://cern.ch/castor/DIST/CONDITIONS
Group: Application/Castor
BuildRoot: %{_builddir}/%{name}-%{version}-root

# RPM specific definitions
#-------------------------
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
echo "castor-devel-oracle, castor-dlf-server, castor-lib-oracle, castor-lsf-plugin, castor-ns-server, castor-rh-server, castor-repack-server, castor-rtcopy-clientserver, castor-stager-server, castor-upv-server, castor-vmgr-server, castor-rmmaster-server, castor-jobmanager-server castor-lib-policy castor-mighunter-server"
for this in BuildCupvDaemon BuildDlfDaemon BuildNameServerDaemon BuildRHCpp BuildRepack BuildRtcpclientd BuildSchedPlugin BuildVolumeMgrDaemon UseOracle UseScheduler BuildOraCpp BuildStageDaemonCpp BuildVDQMCpp BuildDbTools BuildRmMasterCpp BuildJobManagerCpp BuildInfoPolicyLibrary BuildMigHunterDaemon BuildRecHandlerDaemon; do
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
echo "castor-lsf-plugin, castor-rmmaster-server, castor-jobmanager-server"
for this in BuildSchedPlugin BuildRmMasterCpp BuildJobManagerCpp BuildJob; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
%if ! %has_stk_ssi && ! %compiling_nostk
echo "### Warning, no STK environment"
echo "The following packages will NOT be built:"
echo "castor-tape-server"
for this in BuildTapeDaemon; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
%if %compiling_nostk
echo "### Warning, compiling only castor-tape-server-nostk"
for this in BuildCommands BuildCupvClient BuildCupvDaemon BuildCupvLibrary BuildDlfDaemon BuildDlfLibrary BuildDlfWeb BuildExpertClient BuildExpertDaemon BuildExpertLibrary BuildGCCpp BuildHsmTools BuildJobManagerCpp BuildNameServerClient BuildNameServerDaemon BuildNameServerLibrary BuildRHCpp BuildRfioClient BuildRfioLibrary BuildRfioServer BuildRmMasterCpp BuildRmNodeCpp BuildRmcLibrary BuildRmcServer BuildRtcopyClient BuildRtcopyServer BuildRtcpclientd BuildRtstat BuildSchedPlugin BuildSecureCns BuildSecureCupv BuildSecureRfio BuildSecureRtcopy BuildSecureStage BuildSecureTape BuildSecureVdqm BuildSecureVmgr BuildStageClient BuildStageClientOld BuildStageDaemonCpp BuildTapeClient BuildTapeLibrary BuildTpusage BuildVdqmClient BuildVdqmLibrary BuildVolumeMgrClient BuildVolumeMgrDaemon BuildVolumeMgrLibrary BuildVDQMCpp BuildRepack BuildMigHunterDaemon BuildRecHandlerDaemon BuildInfoPolicyLibrary BuildGridFTP HasCDK; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
%if ! %has_globus
echo "### Warning, no GLOBUS environment"
echo "The following packages will NOT be built:"
echo "castor-gridftp-dsi-int, castor-gridftp-dsi-ext, castor-gridftp-dsi-common"
for this in BuildGridFTP; do
	perl -pi -e "s/$this(?: |\t)+.*(YES|NO)/$this\tNO/g" config/site.def
done
%endif
find . -type f -exec touch {} \;
make -f Makefile.ini Makefiles
which makedepend >& /dev/null
[ $? -eq 0 ] && make depend
(cd h; ln -s . shift)
make
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
mkdir -p ${RPM_BUILD_ROOT}/usr/%{LIB}/rtcopy
mkdir -p ${RPM_BUILD_ROOT}/%{_python_lib}
mkdir -p ${RPM_BUILD_ROOT}/usr/lib/perl/CASTOR
mkdir -p ${RPM_BUILD_ROOT}/usr/include/shift
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man1
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man3
mkdir -p ${RPM_BUILD_ROOT}/usr/share/man/man4
mkdir -p ${RPM_BUILD_ROOT}/etc/castor
mkdir -p ${RPM_BUILD_ROOT}/etc/castor/expert
mkdir -p ${RPM_BUILD_ROOT}/etc/castor/policies
mkdir -p ${RPM_BUILD_ROOT}/etc/sysconfig
mkdir -p ${RPM_BUILD_ROOT}/etc/init.d
mkdir -p ${RPM_BUILD_ROOT}/etc/logrotate.d
mkdir -p ${RPM_BUILD_ROOT}/usr/lsf/%{LIB}
mkdir -p ${RPM_BUILD_ROOT}/usr/lsf/etc
mkdir -p ${RPM_BUILD_ROOT}/var/spool/dlf
mkdir -p ${RPM_BUILD_ROOT}/var/spool/expert
mkdir -p ${RPM_BUILD_ROOT}/var/spool/gc
mkdir -p ${RPM_BUILD_ROOT}/var/spool/job
mkdir -p ${RPM_BUILD_ROOT}/var/spool/jobmanager
mkdir -p ${RPM_BUILD_ROOT}/var/spool/monitor
mkdir -p ${RPM_BUILD_ROOT}/var/spool/mighunter
mkdir -p ${RPM_BUILD_ROOT}/var/spool/msg
mkdir -p ${RPM_BUILD_ROOT}/var/spool/ns
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rfio
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rhserver
mkdir -p ${RPM_BUILD_ROOT}/var/spool/rechandler
mkdir -p ${RPM_BUILD_ROOT}/var/spool/repack
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
mkdir -p ${RPM_BUILD_ROOT}/var/log
mkdir -p ${RPM_BUILD_ROOT}/var/lib/castor
mkdir -p ${RPM_BUILD_ROOT}/var/www/html/dlf/db
mkdir -p ${RPM_BUILD_ROOT}/var/www/html/dlf/js
mkdir -p ${RPM_BUILD_ROOT}/var/www/html/dlf/images
mkdir -p ${RPM_BUILD_ROOT}/var/www/conf/dlf/
%if %has_globus
  mkdir -p ${RPM_BUILD_ROOT}/etc/xinetd.d
  install -m 644 gridftp2/external/gsiftp ${RPM_BUILD_ROOT}/etc/xinetd.d/gsiftp
%if ! %has_globus_location
  mkdir -p ${RPM_BUILD_ROOT}/opt/globus/lib
%else
  mkdir -p ${RPM_BUILD_ROOT}/${GLOBUS_LOCATION}/lib
%endif
  mkdir -p ${RPM_BUILD_ROOT}/var/spool/gridftp
%endif
make install DESTDIR=${RPM_BUILD_ROOT}
make exportman DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
# Install policies
(cd clips; ../imake/imake -I../config DESTDIR=${RPM_BUILD_ROOT}; make install DESTDIR=${RPM_BUILD_ROOT})

# Install example files
for i in debian/*CONFIG; do
    install -m 640 ${i} ${RPM_BUILD_ROOT}/etc/castor/`basename ${i}`.example
done
for i in `find . -name "*.init"`; do
    install -m 755 ${i} ${RPM_BUILD_ROOT}/etc/init.d/`basename ${i} | sed 's/\.init//g'`
done
install -m 644 debian/castor.conf ${RPM_BUILD_ROOT}/etc/castor/castor.conf.example
install -m 644 debian/scheduler.py ${RPM_BUILD_ROOT}/etc/castor/policies/scheduler.py.example
install -m 644 debian/migration.py ${RPM_BUILD_ROOT}/etc/castor/policies/migration.py.example
install -m 644 debian/stream.py ${RPM_BUILD_ROOT}/etc/castor/policies/stream.py.example
install -m 644 debian/rechandler.py ${RPM_BUILD_ROOT}/etc/castor/policies/rechandler.py.example
for i in debian/*.logrotate; do
    install -m 644 ${i} ${RPM_BUILD_ROOT}/etc/logrotate.d/`basename ${i} | sed 's/\.logrotate//g'`
done
for i in `find . -name "*.sysconfig"`; do
    install -m 644 ${i} ${RPM_BUILD_ROOT}/etc/sysconfig/`basename ${i} | sed 's/\.sysconfig//g'`.example
done

%if ! %compiling_nostk

# Hardcoded package name CASTOR-client for RPM transition from castor1 to castor2
%package -n CASTOR-client
Summary: Cern Advanced mass STORage
Group: Application/Castor
Requires: castor-stager-clientold,castor-lib,castor-doc,castor-rtcopy-client,castor-upv-client,castor-rtcopy-messages,castor-ns-client,castor-stager-client,castor-vdqm2-client,castor-rfio-client,castor-vmgr-client,castor-devel,castor-tape-client,castor-lib-compat
%description -n CASTOR-client
castor (Cern Advanced STORage system)  Meta package for CASTOR-client from castor1 transition to castor2
%files -n CASTOR-client


# The following will be filled dynamically with the rule: make rpm, or make tar

# Generic macros
#---------------
%define name castor
%define version __A__.__B__.__C__
%define release __D__

# Partial compilations
#---------------------
%{expand:%define compiling_nostk  %(if [ -z $CASTOR_NOSTK ]; then echo 0; else echo 1; fi)}
%{expand:%define compiling_client %(if [ -z $CASTOR_CLIENT ]; then echo 0; else echo 1; fi)}

# Python definitions
#-------------------
%define _python_lib %(python -c "from distutils import sysconfig; print sysconfig.get_python_lib()")

# General settings
#-----------------
Summary: Cern Advanced mass STORage
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.%{release}.tar.gz
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
make -f Makefile.ini Makefiles
(cd h; ln -s . shift)
%if 0%{?clientonly:1} > 0
%define compiling_client 1
%endif
%if %compiling_nostk
echo "Compiling in NOSTK mode, that is only tape part with no STK dependency"
make -j $((`grep processor /proc/cpuinfo | wc -l`*2)) tape
# XXX to be removed when parallel compilation is fixed
make tape
%else
%if %compiling_client
echo "Only compiling client part"
make -j $((`grep processor /proc/cpuinfo | wc -l`*2)) client
# XXX to be removed when parallel compilation is fixed
make client
%else
make -j $((`grep processor /proc/cpuinfo | wc -l`*2))
# XXX to be removed when parallel compilation is fixed
make
%endif
%endif

%install
# define castor version (modified by maketar.sh to put the exact version)
MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__
MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION
rm -rf ${RPM_BUILD_ROOT}
%if %compiling_nostk
make installtape DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
%else
%if %compiling_client
make installclient DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
%else
make install DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
%endif
%endif

%if ! %compiling_nostk

# The following will be filled dynamically with the rule: make rpm, or make tar

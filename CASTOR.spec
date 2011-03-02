# Generic macros
#---------------
%define name castor
%define version __A__.__B__.__C__
%define release __D__

# Partial compilations
#---------------------

%{expand:%define compiling_nostk  %(if [ -z $CASTOR_NOSTK ]; then echo 0; else echo 1; fi)}
%{expand:%define compiling_client %(if [ -z $CASTOR_CLIENT ]; then echo 0; else echo 1; fi)}

# General settings
#-----------------
Summary: Cern Advanced mass STORage
Name: %{name}
Version: %{version}
Release: %{release}
Source: %{name}-%{version}-%{release}.tar.gz
URL: http://cern.ch/castor
License: http://cern.ch/castor/DIST/CONDITIONS
Group: Application/Castor
BuildRoot: %{_builddir}/%{name}-%{version}-root
# only build debug info if you're building the whole code
%if %compiling_client
%define debug_package %{nil}
%endif

%description
The CASTOR Project stands for CERN Advanced STORage Manager, and its goal is to handle LHC data in a fully distributed environment.

%prep
%setup -q

%build
# rpmbuild --define="compilenostk 1" ...
%if 0%{?compilenostk:1} > 0
%define compiling_nostk 1
export CASTOR_NOSTK=%compiling_nostk
%endif

# rpmbuild --define "clientonly 1" ...
%if 0%{?clientonly:1} > 0
%define compiling_client 1
export CASTOR_CLIENT=%compiling_client
# Suppress oracompile.py warnings in clientonly mode
export ORACOMPILE_OPTIONS="--quiet"
%endif

# define castor version (modified by maketar.sh to put the exact version)
MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__
MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION
./configure
%if %compiling_client
(cd h; ln -s . shift)
echo "Only compiling client part"
%{__make} -s %{_smp_mflags} client
%else
%{__make} -s %{_smp_mflags}
%endif

%install
# define castor version (modified by maketar.sh to put the exact version)
MAJOR_CASTOR_VERSION=__MAJOR_CASTOR_VERSION__
MINOR_CASTOR_VERSION=__MINOR_CASTOR_VERSION__
export MAJOR_CASTOR_VERSION
export MINOR_CASTOR_VERSION
%{__rm} -rf ${RPM_BUILD_ROOT}
%if %compiling_client
%{__make} installclient DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
%else
%{__make} install DESTDIR=${RPM_BUILD_ROOT} EXPORTMAN=${RPM_BUILD_ROOT}/usr/share/man
%endif

%clean
%{__rm} -rf $RPM_BUILD_ROOT
%{__rm} -rf $RPM_BUILD_DIR/%{name}-%{version}

# The following will be filled dynamically with the rule: make rpm, or make tar

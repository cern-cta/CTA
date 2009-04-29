Summary:     The next generation xrootd@Castor2 interface utility clients
Name: 	     xrootd-xcastor2util
Version:     1.0.6
Release:     3
License:     none
Group:       Applications/Castor
Source0:     xrootd-xcastor2fs-%{version}.tar.gz
BuildRoot:   %{_builddir}/%{name}-%{version}-root

AutoReqProv: no
Requires:    xrootd

%define __check_files %{nil}
%define debug_package %{nil}

%description
x2cp, stagerget & stagerqry via xrootd

%prep
%setup -n xrootd-xcastor2fs-%{version}

%build
./configure --with-castor-source-location=/opt/xrootd/src/CASTOR2/ --with-xrootd-location=/opt/xrootd/ --prefix=/opt/xrootd  
make -j  8

%install
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf $RPM_BUILD_DIR/%{name}-%{version}

%files
%defattr(-,root,root,-)
/opt/xrootd/bin/xrdstager_get
/opt/xrootd/bin/xrdstager_qry
/opt/xrootd/bin/x2cp
%changelog
* Fri Mar 06 2009 root <root@pcitsmd01.cern.ch> - xcastor2util-1.0.5-1
- Initial build.


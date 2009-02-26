Summary:     The next generation xrootd@Castor2 interface
Name: 	     xrootd-xcastor2fs
Version:     1.0.5
Release:     1
License:     none
Group:       Applications/Castor
Source0:     %{name}-%{version}.tar.gz
BuildRoot:   %{_builddir}/%{name}-%{version}-root

AutoReqProv: no
Requires:    castor-lib >= 2.1.8
Requires:    xrootd

%define __check_files %{nil}
%define debug_package %{nil}

%description
A complete interface to Castor for xrootd with scheduled writes and scheduled or not-scheduled reads.

%prep
%setup

%build
./configure --with-castor-source-location=/opt/xrootd/src/CASTOR2/ --with-xrootd-location=/opt/xrootd/ --prefix=/opt/xrootd  
make -j  8

%install
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf $RPM_BUILD_DIR/%{name}-%{version}

%post
/sbin/chkconfig --add xrd
/sbin/service xrd condrestart > /dev/null 2>&1

%preun
if [ $1 = 0 ]; then
        /sbin/service xrd stop > /dev/null 2>&1
        /sbin/chkconfig --del xrd
fi

%files
%defattr(-,root,root,-)
/opt/xrootd/bin/x2castorjob
/opt/xrootd/lib/lib*.so*

%config(noreplace) /etc/xrd.cf
%config(noreplace) /etc/sysconfig/xrd.example
%config(noreplace) /etc/logrotate.d/xrd
%_sysconfdir/rc.d/init.d/xrd
%_sysconfdir/logrotate.d/xrd

%dir /var/log/xroot/empty
%attr(-,stage,st) %dir /var/log/xroot/server
%attr(-,stage,st) %dir /var/log/xroot/server/proc
%attr(-,stage,st) %dir /var/spool/xroot/admin
%attr(-,stage,st) %dir /var/spool/xroot/core

%changelog
* Thu Feb 26 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.5-1
- added support for adler32 checksumming
-- new config tag: x2castor.checksum always|never|streaming
-- if a file is written sequential the checksum will be computed on the fly for always|streaming
-- if a file is updated the checksum will be recomputed during the close for always

* Tue Feb 03 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.4-2
- converted ROLEMAP/GETID/GETALLGROUPS from XCFS prototype with properl hash protections and security context caching
- added automatic stager settings from open function to stat function & fixed 'CANBEMIRG' typo
  
* Wed Oct 15 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.2-13
- Cleanup of spec file

* Wed Aug 20 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.0-1
- Initial build.


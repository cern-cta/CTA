Summary:     The next generation xrootd@Castor2 interface
Name: 	     xrootd-xcastor2fs
Version:     1.0.6
Release:     11
License:     none
Group:       Applications/Castor
Source0:     %{name}-%{version}.tar.gz
BuildRoot:   %{_builddir}/%{name}-%{version}-root

AutoReqProv: no
Requires:    castor-lib >= 2.1.8-6
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
%config(noreplace) /etc/xrd.cf.meta
%config(noreplace) /etc/sysconfig/xrd.example
%config(noreplace) /etc/logrotate.d/xrd
%_sysconfdir/rc.d/init.d/xrd
%_sysconfdir/rc.d/init.d/cmsd
%_sysconfdir/logrotate.d/xrd

%dir /var/log/xroot/empty
%attr(-,stage,st) %dir /var/log/xroot/server
%attr(-,stage,st) %dir /var/log/xroot/server/proc
%attr(-,stage,st) %dir /var/spool/xroot/admin
%attr(-,stage,st) %dir /var/spool/xroot/core

%changelog
* Tue Jun 02 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-11
- reimplemented inter-process communication between xroot and x2castorjob for 'write'
  via files - no SIGNALs anymore to avoid race conditions under load

* Fri May 29 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-10
- allow now full arbitrary name space mapping, not only first subdirectory

* Tue May 19 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-9
- fixed typo in service class environment variable (serviceClass->svcClass)
  used in sequential rules
- added support to use wildcards in stagemap entries
- fixed logical error for hsm mode. Reinserted StagerQuery before Prepare2Get

* Mon May 11 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-8
- fixed illegal function to retrieve prepare2get response status
  using 'stage_requestStatusName' instead of 'stage_fileStatusName'

* Thu May 07 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-7
- rebuild for wrong stager headers used in the build process
- fixed \0 termination in use of location link cache

* Mon May 04 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-5
- added support for multiple capability keys to provide signature to multiple instances with individual public keys
- fixed bug during file creation for status PUT_FAILED (returns 0 and no serrno)

* Wed Apr 27 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-3
- added support in disk server plugin to set the file size in the nameserver during the close
  (otherwise the typical delay before the namespace size is updated is 3 s)
* Mon Apr 27 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-2
- added support for stagemap ordered lists
- policies can now be defined on client ID + path basis
- policies allow now also 'nostage' and 'ronly'
- fixed several errno->serrno typos
- fixed bug with 'mkdir' (EEXIST was not handled correctly)
- enabled 'dirlist' etc. via 'xrd' client

* Fri Mar 06 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.5-1
- added support for adler32 checksumming
-- new config tag: x2castor.checksum always|never|streaming
-- if a file is written sequential the checksum will be computed on the fly for always|streaming
-- if a file is updated the checksum will be recomputed during the close for always
- cmsd startup script included and global redirector config file /etc/xrd.cf.meta
-  fixes in XMI plugin
-- XMI->Stat true if file exists and staged

* Tue Feb 03 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.4-2
- converted ROLEMAP/GETID/GETALLGROUPS from XCFS prototype with properl hash protections and security context caching
- added automatic stager settings from open function to stat function & fixed 'CANBEMIRG' typo
  
* Wed Oct 15 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.2-13
- Cleanup of spec file

* Wed Aug 20 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.0-1
- Initial build.


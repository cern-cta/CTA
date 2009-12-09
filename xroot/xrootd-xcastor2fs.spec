Summary:     The next generation xrootd@Castor2 interface
Name: 	     xrootd-xcastor2fs
Version:     1.0.9
Release:     3
License:     none
Group:       Applications/Castor
Source0:     %{name}-%{version}.tar.gz
BuildRoot:   %{_builddir}/%{name}-%{version}-root

AutoReqProv: no
Requires:    castor-lib >= 2.1.8-6
Requires:    xrootd
Requires:    xrootd-libtransfermanager

%define debug_package %{nil}
%define __check_files %{nil}

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
/opt/xrootd/bin/x2proc
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
* Tue Dec 09 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.9-3
- adding wildcard support for VOMS group mapping e.g. /atlas/* => zg

* Wed Nov 25 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.9-2
- added stage_setid to the StagerQuery call

* Thu Oct 29 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-10
- preventing bug resulting in infinite loop if user specifies no serviceclass

* Wed Oct 28 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-9
- fixed deadlock Mutex when using GridMap files
- handling properly gridmap-file mapping with CN=... appended when client uses a proxy certificate

* Mon Oct 12 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.9-1
- added support to delete a single replica using "?stagermreplica"
- fixed door for localhost tape access 

* Mon Jun 29 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-6
- remove x2castorjob script and introduce direct socket communication with stagerjob from the OFS
- fixed memory leaks in Put and Rm, StagerQry everytime an error is returned or an exception is catched

* Fri Jun 26 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-5
- fixed again memory leak in StagerQuery command - previous fix didn't work as expected

* Fri Jun 26 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-4
- fixed memory leak in StagerQuery command

* Wed Jun 10 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-3
- put 'keepalive' option in default xrd.cf for disk servers

* Sun May 31 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.8-1
- changed the wrapper script to use state files instead of signals
- removed the set filesize on close option - the close waits 30 seconds for the wrapper script to commit filesize+checksum

* Fri May 29 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.6-10
- allow now full arbitrary name space mapping, not only first subdirectory

* Mon May 25 2009 root <root@pcitsmd01.cern.ch> - xcastor2-1.0.7-1
- introduced 3rd party copy
- introduced localhost connections without authorization (for gridFTP access)
- added x2proc script to change proc settings on local host

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


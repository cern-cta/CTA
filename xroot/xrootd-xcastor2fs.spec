Summary: The next generation xrootd@Castor2 interface
Name: xrootd-xcastor2fs
Version: 1.0.2
Release: 13
License: none
Group: Applications/Castor
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

AutoReqProv: no
Requires: castor-lib >= 2.1.8
Requires: xrootd

%description
A complete interface to Castor for xrootd with scheduled writes and scheduled or not-scheduled reads.
%prep
%setup -q

%build
./configure --with-castor-source-location=/opt/xrootd/src/CASTOR2/ --with-xrootd-location=/opt/xrootd/ --prefix=/opt/xrootd  
make 

%install
make install DESTDIR=$RPM_BUILD_ROOT
rm -f $RPM_BUILD_ROOT/etc/init.d/x2castormond
rm -f $RPM_BUILD_ROOT/opt/xrootd/bin/x2castormond
rm -f $RPM_BUILD_ROOT/opt/xrootd/bin/x2castormonitoring.pl
rm -f $RPM_BUILD_ROOT/opt/xrootd/bin/x2cp
rm -f $RPM_BUILD_ROOT/opt/xrootd/bin/xrdstager_get
rm -f $RPM_BUILD_ROOT/opt/xrootd/bin/xrdstager_qry

find $RPM_BUILD_ROOT \( -type f -o -type l \) -print \
    | sed "s#^$RPM_BUILD_ROOT/*#/#" > RPM-FILE-LIST

%clean
rm -rf $RPM_BUILD_ROOT


%files -f RPM-FILE-LIST
%defattr(-,root,root,-)
%doc

%config(noreplace) /etc/xrd.cf

%changelog
* Wed Oct 15 2008 root <root@pcitsmd01.cern.ch> - xcastor2-13
- Cleanup of spec file

* Wed Aug 20 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1
- Initial build.

%post
mkdir -p /var/log/xroot/server/
cat /etc/passwd | cut -d ':' -f 1 | grep -w stage >& /dev/null && chown stage /var/log/xroot/server/ || echo >& /dev/null

if [ "$1" = "2" ] ; then  # upgrade
        /sbin/service xrd condrestart > /dev/null 2>&1 || :
fi




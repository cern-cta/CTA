
Summary: Monitoring Service for x2castorfs head nodes
Name: xrootd-castormon
Version: 1.0.2
Release: 3
License: none
Group: CERN-IT-DM
Source0: xrootd-xcastor2fs-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
AutoReqProv: no
Requires: perl-Inline-Python 
Requires: xrootd-xcastor2fs
Requires: cx_Oracle

%description
Monitoring Service for x2castorfs head nodes. Includes monitoring service /opt/xrootd/bin/x2castormond and service script /etc/init.d/x2castormond

%prep
%setup -n xrootd-xcastor2fs-1.0.2
./configure --prefix=/opt/xrootd/ --with-xrootd-location=/opt/xrootd/ --with-castor-source-location=/opt/xrootd/src/CASTOR2/
%build

%install
make x2castormond
make mon-install DESTDIR=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT \( -type f -o -type l \) -print \
    | sed "s#^$RPM_BUILD_ROOT/*#/#" > RPM-FILE-LIST

%files -f RPM-FILE-LIST

%defattr(-,root,root,-)
%doc

%clean
rm -rf $RPM_BUILD_ROOT


%post
if [ "$1" = "1" ] ; then  # first install
        /sbin/chkconfig --add x2castormond
        /sbin/service x2castormond start > /dev/null 2>&1 || :
fi

if [ "$1" = "2" ] ; then  # upgrade
        /sbin/service x2castormond condrestart > /dev/null 2>&1 || :
fi

%preun
if [ "$1" = "0" ] ; then  # last deinstall
        [ -e /etc/init.d/x2castormond ] && /etc/init.d/x2castormond stop
        /sbin/chkconfig --del x2castormond
fi


%changelog
* Tue Oct  7 2008 root <peters@pcitsmd01.cern.ch> - castormon-1
- Initial build.


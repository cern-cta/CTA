Summary: A public key for xrootd capability authorization
Name: xrootd-publickey
Version: 1.0.0
Release: 1
License: none
Group: CERN-IT-DM-SMD
Source0: xrootd-xcastor2fs-1.0.2.tar.gz  
BuildRoot: %{_tmppath}/xrootd-xcastor2fs-1.0.2-1-root

AutoReqProv: no
Requires: xrootd

%description
A public key for capability checking on servers.
%prep
%setup -n xrootd-xcastor2fs-1.0.2

%build
./configure --with-castor-source-location=/opt/xrootd/src/CASTOR2/ --with-xrootd-location=/opt/xrootd/ --prefix=/opt/xrootd  

%install
make public-key-install DESTDIR=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT \( -type f -o -type l \) -print \
    | sed "s#^$RPM_BUILD_ROOT/*#/#" > RPM-FILE-LIST

%clean
rm -rf $RPM_BUILD_ROOT


%files -f RPM-FILE-LIST
%defattr(-,root,root,-)
%doc



%changelog
* Wed Aug 20 2008 root <root@pcitsmd01.cern.ch> - xcastor2-1
- Initial build.


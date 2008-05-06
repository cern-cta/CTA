Summary: xrootd-castor interface
Name: xrootd-castor2
Version: 20070802cvs_8_v2.1.7.1
Release: 2
URL: none
Source0: %{name}-%{version}.tar.gz
License: OpenSource
#Prefix: /
Group: CERN
BuildRoot: %{_tmppath}/%{name}-root
#AutoReqProv: no
#%_enable_debug_packages %{nil}
%description
This package contains the castor xrootd plugin e.g. Xmi plugin + XrdCS2d

The software was provided by Andrew Bohdan Hanushevsky [SLAC] (EMAIL: andrew.bohdan.hanushevsky@cern.ch). 
RPM packaging and initd scripts were done by Andreas-Joachim Peters [CERN] (EMAIL: andreas.joachim.peters@cern.ch).

%prep
rm -rf $RPM_BUILD_ROOT
%setup -q
%build
./configure --prefix=/usr/local/xroot/ --disable-static
make
%install
make DESTDIR=$RPM_BUILD_ROOT install
%clean
echo rm -rf $RPM_BUILD_ROOT

%files
/usr/local/xroot
%defattr(-,root,root)

%changelog
* Tue Mar 06 2007 root <root@lxb1388.cern.ch>
- Initial build.
V1.0.1
- Changing to FHS structure
V1.0.2
- Added SVCCLASS switch - fixed directory permission in startup
V1.0.3
- Adding castorns configuration variable to Xmi plugin library
V1.0.6
- Fixing bugs in XrdCS2d
- Adding max fd & core dump size to init scripts
- Setting the debug flag for XrdCS2d too
V2.0.0
- Updating to CVS version xrootd-20070611-0226
V2.1.0
- Removing startup scripts and include only xrootd-castor components
V2.1.4
- compatible with new xrootd structure 
V20070802cvs_4_2.1.4.1
- fix mkpath + STAGEIN status
V20070802cvs_4_2.1.4.4
- fix race condtion in fwrite|open situation 
V2.1.6
- use doPut & doGet - no prepare2get 
V2.1.46
- unification of castor 2.1.4 and 2.1.6 compatible plugin
V2.1.7
- fixes for CASTOR API changes
- added CS2Ofs with rate limiter

%post
%preun
%postun
echo 


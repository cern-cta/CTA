%define name 	  castor-mon-web
%define version   1.0.1
%define release   1
Summary: 	CASTOR Monitoring web-interface
Name: 		%{name}
Version: 	%{version}
Release: 	%{release}
License: 	http://cern.ch/castor/DIST/CONDITIONS
Group:          Application/Castor
URL:            http://cern.ch/castor
Source: 	%{name}-%{version}.tar.gz
BuildRoot: 	%{_tmppath}/%{name}-%{version}
Buildarch:      noarch
Requires:       php >= 4.0.0, httpd, php-oci8, php-gd

%description
This web interface provides visualization of some new statistical metrics for CASTOR.
It consists of two different levels.The first one is a monitor displaying  CASTOR's current state. 
The second one provides several statistical (historical) information.  

%prep

%setup -q -n %{name}-%{version}

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/calendar
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/dashboard
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/images
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/lib
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/stat
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/exp_mon
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/docs/html/exframes
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/docs/html/img
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/docs/ref
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/src/Examples
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/src/lang
mkdir -p $RPM_BUILD_ROOT/var/www/html/castor-mon-web/tmp/jpgraph_cache
mkdir -p $RPM_BUILD_ROOT/var/www/conf/castor-mon-web
install -m 644 html/index.php $RPM_BUILD_ROOT/var/www/html/castor-mon-web/
install -m 644 html/calendar/*.js $RPM_BUILD_ROOT/var/www/html/castor-mon-web/calendar/
install -m 644 html/dashboard/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/dashboard/
install -m 644 html/images/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/images/
install -m 644 html/lib/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/lib/
install -m 644 html/stat/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/stat/
install -m 644 html/exp_mon/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/exp_mon/
install -m 640 conf/* $RPM_BUILD_ROOT/var/www/conf/castor-mon-web/
install -m 644 jpgraph-1.27/QPL.txt $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/
install -m 644 jpgraph-1.27/README $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/
install -m 644 jpgraph-1.27/VERSION $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/
install -m 644 jpgraph-1.27/src/*.* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/src/
install -m 644 jpgraph-1.27/src/lang/* $RPM_BUILD_ROOT/var/www/html/castor-mon-web/jpgraph-1.27/src/lang/
%clean

%files
%defattr(-,root,root,-)
/var/www/html/castor-mon-web/
/var/www/conf/castor-mon-web/
%attr(-,apache,apache) /var/www/html/castor-mon-web/tmp/jpgraph_cache
%attr(-,root,apache) /var/www/conf/castor-mon-web/user.php.example

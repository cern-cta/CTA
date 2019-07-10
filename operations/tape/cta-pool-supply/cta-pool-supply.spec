Summary: Daily tape mount statistics report for TSMOD
Name: CERN-CC-tape-mount-statistics
Version: 1.3
Release: 1
License: GPL
Buildroot: /tmp/%{name}-%{version}
BuildArch: noarch
Group: Application/TSMOD
Source: %{name}-%{version}.tgz

%description
This RPM contains a script that will generate an tape mount
statistics report on the tape infrastructure since the specified
date.

TSMOD should investigate whether there are some users misusingthe
service.

It should be run at most twice per day.

TSMOD = Tape Service Manager on Duty

Author: Vladimir Bahyl - 11/2014
%prep

%setup -c

%install
[ -d $RPM_BUILD_ROOT ] && rm -rf $RPM_BUILD_ROOT
mkdir $RPM_BUILD_ROOT
cd $RPM_BUILD_ROOT
mkdir -p usr/local/bin etc/cron.d
name=`echo %{name} |sed 's/CERN-CC-//'`

install -m 755 $RPM_BUILD_DIR/%{name}-%{version}/tape-mount-statistics.sh	usr/local/bin/tape-mount-statistics.sh
install -m 644 $RPM_BUILD_DIR/%{name}-%{version}/tape-mount-statistics.cron	etc/cron.d/tape-mount-statistics.cron

%post

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/local/bin/tape-mount-statistics.sh
/etc/cron.d/tape-mount-statistics.cron

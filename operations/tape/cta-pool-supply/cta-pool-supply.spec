Summary: CTA supply re-fill mechanism
Name: cta-pool-supply
Version: 1.0
Release: 1
License: GPL
Buildroot: /tmp/%{name}-%{version}
BuildArch: noarch
Group: Application/CTA
Requires: python36 cta-cli
Source: %{name}-%{version}.tgz

%description
This RPM contains a script that will generate an tape mount
statistics report on the tape infrastructure since the specified
date.

TSMOD should investigate whether there are some users misusingthe
service.

It should be run at most twice per day.

TSMOD = Tape Service Manager on Duty

Author: Vladimir Bahyl - 7/2019
%prep

%setup -c

%install
[ -d $RPM_BUILD_ROOT ] && rm -rf $RPM_BUILD_ROOT
mkdir $RPM_BUILD_ROOT
cd $RPM_BUILD_ROOT
mkdir -p usr/local/bin etc/cron.d
name=`echo %{name} |sed 's/CERN-CC-//'`

install -m 755 $RPM_BUILD_DIR/%{name}-%{version}/cta-pool-supply	usr/local/bin/cta-pool-supply
install -m 644 $RPM_BUILD_DIR/%{name}-%{version}/cta-pool-supply.cron	etc/cron.d/cta-pool-supply.cron

%post

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/local/bin/cta-pool-supply
/etc/cron.d/cta-pool-supply.cron
/var/log/cta-pool-supply.log

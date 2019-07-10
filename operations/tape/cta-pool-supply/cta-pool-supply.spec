Summary: CTA pool supply re-fill mechanism
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
CTA pool supply re-fill mechanism

* CTA tape pools should have at least X partial tapes available for writing.
* Eligible partial tapes are those that are not DISABLED, not READONLY,
  not FULL and not in a DISABLED logical tape library.
* If the number of eligible partial tapes of a given tape pool falls below the
  configured limit, the pool needs to be re-supplied with fresh tapes.
* Fresh supply tapes are taken from tape pools defined in the "supply" column.
* There can be multiple supply tape pools and they can be separated by
  a separator (usually comma).
* There is no distinction between what is a supply pool and what is not, if
  a pool has a value in the "supply" column, tapes are taken from that pool.
  Because of this, irregularities, cyclical loops and other misconfiguration
  are possible - please be careful.
* This script is intended to run every 15 minutes.

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

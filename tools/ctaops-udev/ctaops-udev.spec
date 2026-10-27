# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

Summary: CTA Udev Rules
Name: ctaops-udev
Version: %{_VERSION}
Release: %{_RELEASE}
License: GPL-3.0-or-later
Buildroot: %{_tmppath}/%{name}-%{version}
Source: %{name}-%{version}.tar.gz
BuildArch: noarch
Group: Application/CTA
BuildRequires: pandoc
Requires: python3 systemd-udev

%description
CTA Udev rules:
 - Add udev rule for managing symlink
 - Add ctaops-udev script for this rule
 - Add ctaops-udev.ini conf file

Author: CERN TAPE Archive eoscta-service-ops@cern.ch - 12/2024

%prep
%setup -n %{name}-%{version}

%build
pandoc -s -t man ctaops-udev.8ctaops.md -o ctaops-udev.8ctaops

%install
[ -d %{buildroot} ] && rm -rf %{buildroot}

mkdir -p %{buildroot}/usr/lib/udev/rules.d/ %{buildroot}%{_mandir}/man8
install -m 644 -p src/62-ctaops.rules      %{buildroot}/usr/lib/udev/rules.d/62-ctaops.rules
install -m 644 -p src/ctaops-udev.ini   %{buildroot}/usr/lib/udev/ctaops-udev.ini
install -m 755 -p src/ctaops_udev.py   %{buildroot}/usr/lib/udev/ctaops-udev
install -m 644 ctaops-udev.8ctaops %{buildroot}%{_mandir}/man8/ctaops-udev.8ctaops

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%license LICENSE
/usr/lib/udev/rules.d/62-ctaops.rules
/usr/lib/udev/ctaops-udev
%{_mandir}/man8/ctaops-udev.8ctaops*
%config(noreplace) /usr/lib/udev/ctaops-udev.ini


%post
/usr/sbin/udevadm control --reload-rules && /usr/sbin/udevadm trigger

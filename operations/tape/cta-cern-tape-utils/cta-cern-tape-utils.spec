Summary: CTA CERN specific tape utilities
Name: cta-cern-tape-utils
Version: 1.0
Release: 2
License: GPL
Buildroot: /tmp/%{name}-%{version}
BuildArch: noarch
Group: Application/CTA
Requires: python36 cta-cli
Source: %{name}-%{version}.tgz

%global __python %{__python3}

%description
CTA CERN specific tape utilities:
 - generate /etc/cta/TPCONFIG file from TOMS framework
 - tape mount/unmount script
 - tape label wrapper
 - tape media check
 - tape drive test

Author: David Fernandez Alvarez, Vladimir Bahyl - 8/2019
%prep

%setup -c

%install
[ -d $RPM_BUILD_ROOT ] && rm -rf $RPM_BUILD_ROOT
mkdir $RPM_BUILD_ROOT
cd $RPM_BUILD_ROOT
mkdir -p usr/local/bin usr/local/sbin usr/local/lib/python3.6
name=`echo %{name} |sed 's/CERN-CC-//'`

install -m 755 $RPM_BUILD_DIR/%{name}-%{version}/tape-config-generate				$RPM_BUILD_ROOT/usr/local/bin/tape-config-generate
install -m 755 $RPM_BUILD_DIR/%{name}-%{version}/tape-drive-configure-SCSI-mode-pages-CERN.sh	$RPM_BUILD_ROOT/usr/local/sbin/tape-drive-configure-SCSI-mode-pages-CERN.sh

install -m 644 -p $RPM_BUILD_DIR/%{name}-%{version}/TapeAdmin.py				$RPM_BUILD_ROOT/usr/local/lib/python3.6

install -m 755 -p $RPM_BUILD_DIR/%{name}-%{version}/tape-mount					$RPM_BUILD_ROOT/usr/local/bin/tape-mount
ln -s tape-mount										$RPM_BUILD_ROOT/usr/local/bin/tape-unmount
install -m 755 -p $RPM_BUILD_DIR/%{name}-%{version}/tape-label					$RPM_BUILD_ROOT/usr/local/bin/tape-label

%post

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/local/bin/tape-config-generate
/usr/local/sbin/tape-drive-configure-SCSI-mode-pages-CERN.sh
/usr/local/lib/python3.6/TapeAdmin.py
/usr/local/lib/python3.6/__pycache__/TapeAdmin.cpython-36.opt-1.pyc
/usr/local/lib/python3.6/__pycache__/TapeAdmin.cpython-36.pyc
/usr/local/bin/tape-mount
/usr/local/bin/tape-unmount
/usr/local/bin/tape-label

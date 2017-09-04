Summary: CERN Tape Archive workflow scripts for EOS
Name: cta_eos_wfe_scripts
Version: 0.1
Release: 1
License: GPL
Group: Applications/System
Buildroot: %{_tmppath}/%{name}-%{version}
Source: %{name}-%{version}.tar.gz
Group: Applications/System
BuildArch: noarch
requires: eos-server

%description
eos_wfe_scripts contains all the workflows needed for archival from EOS to CTA and for retrieva from CTA to EOS.
This version contains all the file for the so called *preproduction* workflows.


%prep
%setup -n %{name}-%{version}


%build


%install
[ -d %{buildroot} ] && rm -rf %{buildroot}

mkdir -p %{buildroot}/var/eos/wfe/bash
install -m 755 create_tape_drop_disk_replicas %{buildroot}/var/eos/wfe/bash/create_tape_drop_disk_replicas
install -m 755 delete_archive_file %{buildroot}/var/eos/wfe/bash/delete_archive_file
install -m 755 retrieve_archive_file %{buildroot}/var/eos/wfe/bash/retrieve_archive_file


%clean
rm -rf %{buildroot}


%files
%defattr(-,daemon,daemon)
/var/eos/wfe/bash/create_tape_drop_disk_replicas
/var/eos/wfe/bash/delete_archive_file
/var/eos/wfe/bash/retrieve_archive_file

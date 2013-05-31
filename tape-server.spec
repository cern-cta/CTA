Summary: The CERN tape server project
Name: tape-server
Version: 0.0.0
Release: 1
Prefix: /usr
License: GPL
Group: Applications/File

Source: %{name}-%{version}-%{release}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: cmake >= 2.6
BuildRequires: gtest >= 1.5.0
BuildRequires: gmock >= 1.5.0

%description
The CERN tape server project.

#######################################################################################
%package -n tape-server-utils
#######################################################################################
Summary: The CERN tape server utilities
Group: Applications/File

%description -n tape-server-utils
The CERN tape server utilities

%prep

%setup -n %{name}-%{version}-%{release}

%build
test -e $RPM_BUILD_ROOT && rm -r $RPM_BUILD_ROOT
%if 0%{?rhel} < 6 && %{?fedora}%{!?fedora:0} <= 1
export CC=/usr/bin/gcc44 CXX=/usr/bin/g++44 
%endif

mkdir -p build
cd build
cmake ../ -DRELEASE=%{release} -DCMAKE_BUILD_TYPE=RelWithDebInfo
%{__make} %{_smp_mflags} 

%install
cd build
%{__make} install DESTDIR=$RPM_BUILD_ROOT
echo "Installed!"

%check
cd build
test/unitTest

%clean
rm -rf $RPM_BUILD_ROOT

%files -n tape-server-utils
%defattr(-,root,root)
/usr/local/bin/*

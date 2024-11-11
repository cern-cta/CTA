# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

Name: gtest-devel
Version: 1.12.0
Release: 1
Summary: GoogleTest

Group:	CERN
License: BSD 3
URL: https://github.com/google/googletest/archive/refs/tags/v1.12.0.tar.gz
Source0: v%{version}.tar.gz
Buildarch: x86_64
Prefix: %{_prefix}
Buildroot: %{_tmppath}/%{name}-%{version}

%description


%prep
%setup -c -n %{name}-%{version}

%build
mkdir -p build
cd build
cmake3 -DCMAKE_INSTALL_PREFIX="/usr" -DBUILD_SHARED_LIBS=1 ../googletest-%{version}
make

#%define  debug_package %{nil}
%install
%{__rm} -rf ${RPM_BUILD_ROOT}
cd build
make install DESTDIR=%{buildroot}
# copy license
mkdir -p %{buildroot}/usr/share/licenses/gtest
cp ../googletest-%{version}/LICENSE %{buildroot}/usr/share/licenses/gtest/LICENSE

%clean
[ -d %{buildroot} ] && rm -rf %{buildroot}

%files
%license
/usr/share/licenses/gtest/LICENSE
%defattr(-,root,root)
%doc
/usr/include/gmock/
/usr/include/gtest/
/usr/lib64/cmake/GTest
/usr/lib64/libgmock*
/usr/lib64/libgtest*
/usr/lib64/pkgconfig/
%changelog

# The CERN Tape Archive (CTA) project
# Copyright 2019 CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

find_package(xrootd REQUIRED)
find_package(Protobuf3 REQUIRED)

include_directories(${XRD_SSI_PB_DIR}/include ${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

# Upgrade DB tool
add_executable(cta-upgrade-db UpgradeDB.cpp)
target_link_libraries(cta-upgrade-db ctacatalogue)
set_property(TARGET cta-upgrade-db APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-upgrade-db DESTINATION usr/bin)

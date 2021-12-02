# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 3.17)

find_package(xrootd REQUIRED)
find_package(xrootdclient REQUIRED)
find_package(Protobuf3 REQUIRED)

include_directories(${XROOTD_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_executable(cta-client-ar-abortPrepare client-ar-abortPrepare.cpp)
target_link_libraries(cta-client-ar-abortPrepare XrdCl ctacommon)
install(TARGETS cta-client-ar-abortPrepare DESTINATION usr/bin)
set_property(TARGET cta-client-ar-abortPrepare APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

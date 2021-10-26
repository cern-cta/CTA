# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 DESY
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

cmake_minimum_required (VERSION 2.6)

find_package(Protobuf3 REQUIRED)

include_directories(${CMAKE_BINARY_DIR}/eos_cta
                    ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-dcache FrontendGRpcSvc.cpp)

target_link_libraries(cta-dcache CtaGRpc
        ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
        ctascheduler ctacommon ctaobjectstore ctacatalogue)
set_property(TARGET cta-dcache APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-dcache DESTINATION usr/bin)
install (FILES cta-dcache.service DESTINATION etc/systemd/system)
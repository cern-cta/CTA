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
find_package(GRPC REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS})

set(PROTOBUF_CTADCACHE_DIR ${PROJECT_SOURCE_DIR}/cta-dcache/grpc-proto/protobuf)
file(GLOB ProtoFilesCtaDcache "${PROTOBUF_CTADCACHE_DIR}/*.proto")

# depends on CTA_EOS definitions
set(PROTOBUF_EOS_CTA_DIR ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
file(GLOB ProtoFilesEosCta "${PROTOBUF_EOS_CTA_DIR}/*.proto")

set(PROTOBUF3_INCLUDE_PATH "${PROTOBUF_EOS_CTA_DIR}:${PROTOBUF_CTADCACHE_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
PROTOBUF3_GENERATE_CPP(ProtoSourcesCtaDcache ProtoHeadersCtaDcache ${ProtoFilesCtaDcache})

set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/cta-dcache")
set(PROTOBUF3_IMPORT_DIRS "${PROTOBUF_EOS_CTA_DIR}:${PROTOBUF3_INCLUDE_DIRS}")
grpc_generate_cpp(ProtoGrpcSourcesCtaDcache ProtoGrpcHeadersCtaDcache ${GRPC_PROTOBUF_PATH} ${ProtoFilesCtaDcache})

set_source_files_properties(
        ${ProtoSourcesCtaDcache} ${ProtoHeadersCtaDcache}
        ${ProtoGrpcSourcesCtaDcache} ${ProtoGrpcHeadersCtaDcache}
        PROPERTIES GENERATED TRUE)

foreach(PROTO_SRC ${ProtoSourcesCtaDcache} ${ProtoGrpcSourcesCtaDcache})
    set_property(SOURCE ${PROTO_SRC} PROPERTY COMPILE_FLAGS " -Wno-missing-field-initializers -fPIC -Wno-narrowing -Wno-implicit-fallthrough")
    # Add -Wno-narrowing -Wno-implicit-fallthrough compiler flags if using gcc version 7 or greater
    if(CMAKE_COMPILER_IS_GNUCC)
        if(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
            set_property(SOURCE ${PROTO_SRC} APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-narrowing -Wno-implicit-fallthrough")
        endif(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
    endif(CMAKE_COMPILER_IS_GNUCC)
endforeach(PROTO_SRC)

include_directories(${CMAKE_BINARY_DIR}/cta-dcache ${CMAKE_BINARY_DIR}/eos_cta
                    ${PROTOBUF3_INCLUDE_DIRS})

add_executable(cta-dcache FrontendGRpcSvc.cpp ${ProtoSourcesCtaDcache} ${ProtoGrpcSourcesCtaDcache})

target_link_libraries(cta-dcache ${PROTOBUF3_LIBRARIES} ${GRPC_GRPC++_LIBRARY}
        ctascheduler ctacommon ctaobjectstore ctacatalogue)
set_property(TARGET cta-dcache APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-dcache DESTINATION usr/bin)
install (FILES cta-dcache.service DESTINATION etc/systemd/system)
install (FILES cta-dcache.sysconfig DESTINATION /etc/sysconfig RENAME cta-dcache)
install (FILES cta-dcache.1cta DESTINATION /usr/share/man/man1)
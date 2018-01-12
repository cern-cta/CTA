# The CERN Tape Archive (CTA) project
# Copyright 2018 CERN
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
cmake_minimum_required(VERSION 2.6)

find_package(Protobuf3 REQUIRED)
set(PROTOBUF3_INCLUDE_PATH ${XRD_SSI_PB_DIR}/eos_cta/protobuf)
file(GLOB ProtoFiles "${PROTOBUF3_INCLUDE_PATH}/*.proto")

PROTOBUF3_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

set_source_files_properties(
  ${ProtoSources} ${ProtoHeaders}
  PROPERTIES GENERATED 1)

foreach(PROTO_SRC ${ProtoSources})
  set_property(SOURCE ${PROTO_SRC} PROPERTY COMPILE_FLAGS " -Wno-missing-field-initializers -fPIC -Wno-narrowing -Wno-implicit-fallthrough")

  # Add -Wno-narrowing -Wno-implicit-fallthrough compiler flags if using gcc version 7 or greater
  if(CMAKE_COMPILER_IS_GNUCC)
    if(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
      set_property(SOURCE ${PROTO_SRC} APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-narrowing -Wno-implicit-fallthrough")
    endif(GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
  endif(CMAKE_COMPILER_IS_GNUCC)
endforeach(PROTO_SRC)
set(CTA_FRONT_END_MESSAGES_SRC_FILES ${ProtoSources})

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library(XrdSsiPbEosCta ${CTA_FRONT_END_MESSAGES_SRC_FILES})
set_target_properties(XrdSsiPbEosCta PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(XrdSsiPbEosCta ${PROTOBUF3_LIBRARIES})

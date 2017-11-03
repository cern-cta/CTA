# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
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

find_package(Protobuf3 REQUIRED)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF3_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})
foreach(PROTO_SRC ${ProtoSources})
  set_property(SOURCE ${PROTO_SRC} PROPERTY COMPILE_FLAGS " -Wno-missing-field-initializers -fPIC")
endforeach(PROTO_SRC)
set (CTA_FRONT_END_MESSAGES_SRC_FILES ${ProtoSources})

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library (XrdCtaMessages ${CTA_FRONT_END_MESSAGES_SRC_FILES})

target_link_libraries (XrdCtaMessages ${PROTOBUF3_LIBRARIES})

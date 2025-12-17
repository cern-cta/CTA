# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_library(SCSI Device.cpp Structures.cpp Constants.cpp Exception.cpp)

add_library(ctatapeserverscsiunittests SHARED
  DeviceTest.cpp
  StructuresTest.cpp)
set_property(TARGET ctatapeserverscsiunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverscsiunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverscsiunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

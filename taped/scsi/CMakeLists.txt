# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctascsi Device.cpp Structures.cpp Constants.cpp Exception.cpp)

target_link_libraries(ctascsi ctacommon)

add_library(ctascsiunittests SHARED
  DeviceTest.cpp
  StructuresTest.cpp)
set_property(TARGET ctascsiunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascsiunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctascsiunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

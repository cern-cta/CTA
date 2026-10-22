# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctascsi SHARED Device.cpp Structures.cpp Constants.cpp Exception.cpp)
set_target_properties(ctascsi PROPERTIES
  SOVERSION "${CTA_SOVERSION}"
  VERSION "${CTA_LIBVERSION}")
install(TARGETS ctascsi DESTINATION ${CMAKE_INSTALL_LIBDIR})

target_link_libraries(ctascsi ctacommon)

add_library(ctascsiunittests SHARED
  DeviceTest.cpp
  StructuresTest.cpp)
set_property(TARGET ctascsiunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascsiunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS ctascsiunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

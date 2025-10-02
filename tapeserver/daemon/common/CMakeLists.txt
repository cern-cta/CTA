# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

add_library(ctatapedaemoncommon SHARED
  TapedConfiguration.cpp
)

target_link_libraries(ctatapedaemoncommon
  stdc++fs
)

set_property(TARGET ctatapedaemoncommon PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedaemoncommon PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctatapedaemoncommonunittests SHARED
  TapedConfigurationTests.cpp
)

set_property(TARGET ctatapedaemoncommonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedaemoncommonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapedaemoncommon DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctatapedaemoncommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

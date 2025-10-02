# SPDX-FileCopyrightText: 2022 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)

add_library (ctaexceptionunittests SHARED
  ExceptionTest.cpp)
set_property(TARGET ctaexceptionunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaexceptionunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaexceptionunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

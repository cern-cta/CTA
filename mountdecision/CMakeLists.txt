# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

include_directories (${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR})

add_library (ctamountdecision SHARED
  MountDecision.cpp
  MountDecisionDB.cpp)

target_link_libraries (ctamountdecision
  ctacommon
  ctardbms)

set_property(TARGET ctamountdecision PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamountdecision PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctamountdecision DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

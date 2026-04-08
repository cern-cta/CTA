# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_library(System Wrapper.cpp FileWrappers.cpp)
set_property(SOURCE Wrapper.cpp
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)


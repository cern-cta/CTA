# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_library(ctatapedsystem Wrapper.cpp FileWrappers.cpp)
set_property(SOURCE Wrapper.cpp
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)


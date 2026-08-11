# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_library(ctatapedsystem Wrapper.cpp FileWrappers.cpp)
set_property(SOURCE Wrapper.cpp
  APPEND PROPERTY COMPILE_OPTIONS -fno-strict-aliasing
)

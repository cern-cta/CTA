# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     LIBUUID_FOUND
#     LIBUUID_INCLUDE_DIRS
#     LIBUUID_LIBRARIES

find_path(LIBUUID_INCLUDE_DIRS
  uuid.h
  PATH_SUFFIXES uuid)

find_library(LIBUUID_LIBRARIES
  NAMES uuid)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libuuid DEFAULT_MSG
  LIBUUID_INCLUDE_DIRS LIBUUID_LIBRARIES)

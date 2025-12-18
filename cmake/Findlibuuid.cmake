# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     LIBUUID_FOUND
#     LIBUUID_INCLUDE_DIRS
#     LIBUUID_LIBRARIES

find_path(LIBUUID_INCLUDE_DIRS
  uuid.h
  PATHS /usr/include/uuid
  NO_DEFAULT_PATH)

find_library(LIBUUID_LIBRARIES
  NAME uuid
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libuuid DEFAULT_MSG
  LIBUUID_INCLUDE_DIRS LIBUUID_LIBRARIES)

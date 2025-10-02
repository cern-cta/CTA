# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     LIBCAP_FOUND
#     LIBCAP_INCLUDE_DIRS
#     LIBCAP_LIBRARIES

find_path(LIBCAP_INCLUDE_DIRS
  capability.h
  PATHS /usr/include/sys
  NO_DEFAULT_PATH)

find_library(LIBCAP_LIBRARIES
  NAME cap
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcap DEFAULT_MSG
  LIBCAP_INCLUDE_DIRS LIBCAP_LIBRARIES)

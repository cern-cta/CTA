# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     LIBCAP_FOUND
#     LIBCAP_INCLUDE_DIRS
#     LIBCAP_LIBRARIES

find_path(LIBCAP_INCLUDE_DIRS
  capability.h
  PATH_SUFFIXES sys)

find_library(LIBCAP_LIBRARIES
  NAMES cap)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcap DEFAULT_MSG
  LIBCAP_INCLUDE_DIRS LIBCAP_LIBRARIES)

# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     BINUTILS_FOUND
#     BINUTILS_INCLUDE_DIRS
#     BINUTILS_LIBRARIES

find_path(BINUTILS_INCLUDE_DIRS
  bfd.h)

find_library(BINUTILS_LIBRARIES
  NAMES bfd)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(binutils DEFAULT_MSG
  BINUTILS_INCLUDE_DIRS BINUTILS_LIBRARIES)

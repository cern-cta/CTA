# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     LIBRADOS2_FOUND
#     LIBRADOS2_INCLUDE_DIRS
#     LIBRADOS2_LIBRARIES

find_path(LIBRADOS2_INCLUDE_DIRS
  librados.h
  PATHS /usr/include/rados
  NO_DEFAULT_PATH)

find_library(LIBRADOS2_LIBRARIES
  NAME rados
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(librados2 DEFAULT_MSG
  LIBRADOS2_INCLUDE_DIRS LIBRADOS2_LIBRARIES)

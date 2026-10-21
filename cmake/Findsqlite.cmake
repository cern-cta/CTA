# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     SQLITE_FOUND
#     SQLITE_INCLUDE_DIRS
#     SQLITE_LIBRARIES

find_path(SQLITE_INCLUDE_DIRS
  sqlite3.h)

find_library(SQLITE_LIBRARIES
  NAMES sqlite3)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(sqlite DEFAULT_MSG
  SQLITE_INCLUDE_DIRS SQLITE_LIBRARIES)

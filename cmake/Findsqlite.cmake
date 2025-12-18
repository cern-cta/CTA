# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     SQLITE_FOUND
#     SQLITE_INCLUDE_DIRS
#     SQLITE_LIBRARIES

find_path(SQLITE_INCLUDE_DIRS
  sqlite3.h
  PATHS /usr/include
  NO_DEFAULT_PATH)

find_library(SQLITE_LIBRARIES
  NAME sqlite3
  PATHS /usr/lib64
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(sqlite DEFAULT_MSG
  SQLITE_INCLUDE_DIRS SQLITE_LIBRARIES)

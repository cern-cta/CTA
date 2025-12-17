# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
# This module will set the following variables:
#     POSTGRES_FOUND
#     POSTGRES_INCLUDE_DIRS
#     POSTGRES_LIBRARIES

find_path(POSTGRES_INCLUDE_DIRS
  libpq-fe.h
  PATHS /usr/include /usr/pgsql-12/include
  NO_DEFAULT_PATH)

find_library(POSTGRES_LIBRARIES
  NAME pq
  PATHS /usr/lib64/ /usr/pgsql-12/lib/
  NO_DEFAULT_PATH)

message (STATUS "POSTGRES_INCLUDE_DIRS = ${POSTGRES_INCLUDE_DIRS}")
message (STATUS "POSTGRES_LIBRARIES    = ${POSTGRES_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(postgres DEFAULT_MSG
  POSTGRES_INCLUDE_DIRS POSTGRES_LIBRARIES)

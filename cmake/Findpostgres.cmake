# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# This module will set the following variables:
#     POSTGRES_FOUND
#     POSTGRES_INCLUDE_DIRS
#     POSTGRES_LIBRARIES

find_path(POSTGRES_INCLUDE_DIRS
  libpq-fe.h
  PATHS /usr/pgsql-12/include
  PATH_SUFFIXES postgresql)

find_library(POSTGRES_LIBRARIES
  NAMES pq
  PATHS /usr/pgsql-12/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(postgres DEFAULT_MSG
  POSTGRES_INCLUDE_DIRS POSTGRES_LIBRARIES)

# Public PostgreSQL headers carry their include path to consumers.
if(postgres_FOUND AND NOT TARGET PostgreSQL::PostgreSQL)
  add_library(PostgreSQL::PostgreSQL UNKNOWN IMPORTED)
  set_target_properties(PostgreSQL::PostgreSQL PROPERTIES
    IMPORTED_LOCATION "${POSTGRES_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${POSTGRES_INCLUDE_DIRS}")
endif()

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2018-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

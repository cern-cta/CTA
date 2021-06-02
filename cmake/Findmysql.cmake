# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
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
#     MYSQL_FOUND
#     MYSQL_INCLUDE_DIRS
#     MYSQL_LIBRARIES

find_path(MYSQL_INCLUDE_DIRS
  mysql.h
  PATHS /usr/include/mysql
  NO_DEFAULT_PATH)

find_library(MYSQL_LIBRARIES
  NAME mysqlclient
  PATHS /usr/lib64/mysql /usr/lib64
  NO_DEFAULT_PATH)

message (STATUS "MYSQL_INCLUDE_DIRS = ${MYSQL_INCLUDE_DIRS}")
message (STATUS "MYSQL_LIBRARIES    = ${MYSQL_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(mysql DEFAULT_MSG
  MYSQL_INCLUDE_DIRS MYSQL_LIBRARIES)

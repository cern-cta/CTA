
# The CERN Tape Archive(CTA) project
# Copyright(C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# This module will set the following variables:
#     STK_FOUND
#     STK_LIBRARIES
#     STK_INCLUDE_DIR

find_library (STK_LIBRARIES
  stk-ssi
  PATHS /usr/lib64/CDK /usr/lib/CDK
  NO_DEFAULT_PATH)

find_path (STK_INCLUDE_DIRS
  acssys.h
  PATHS /usr/include/CDK
  NO_DEFAULT_PATH)

message (STATUS "STK_LIBRARIES=${STK_LIBRARIES}")
message (STATUS "STK_INCLUDE_DIRS=${STK_INCLUDE_DIRS}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(stk DEFAULT_MSG
  STK_LIBRARIES
  STK_INCLUDE_DIRS)

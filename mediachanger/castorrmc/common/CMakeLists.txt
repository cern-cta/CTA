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
cmake_minimum_required (VERSION 3.17)

include_directories (${PROJECT_SOURCE_DIR}/mediachanger/castorrmc/h)

set (CTARMCCOMMON_LIB_SRC_FILES
  Cdomainname.c
  Cglobals.c
  Cinitdaemon.c
  Cnetdb.c
  Csnprintf.c
  getconfent.c
  marshall.c
  readc.c
  serror.c
  socket.c
  socket_timeout.c
  strerror_r_wrapper.cpp
  util.c)

add_library(ctarmccommon ${CTARMCCOMMON_LIB_SRC_FILES})

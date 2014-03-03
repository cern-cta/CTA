#
#                      common/CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

set (COMMON_LIB_SRC_FILES
  compat.c getacct.c getacctent.c getconfent.cpp
  getifnam.c getuser.c log.c marshall.c
  readc.c remote.c
  serror.c socket.c solveln.c trace.c
  ypgetacct.c ypgetacctent.c
  Cthread.c Cglobals.c Cgrp.c Cmutex.c Cuuid.c
  Cdomainname.c Cnetdb.c Cpool.c Cpwd.c Cstrerror.c
  socket_timeout.c u64subr.c Cinitdaemon.c Cgetopt.c
  Cregexp.c Cexportconf.c Csnprintf.c util.c
  Csecloader.c castor1compat.c
  ../castor/exception/AcceptConnectionInterrupted.cpp
  ../castor/exception/AlreadyInitialized.cpp
  ../castor/exception/BadAlloc.cpp
  ../castor/exception/Backtrace.cpp
  ../castor/exception/BadVersion.cpp
  ../castor/exception/Busy.cpp
  ../castor/exception/CommandLineNotParsed.cpp
  ../castor/exception/DismountFailed.cpp
  ../castor/exception/Errnum.cpp
  ../castor/exception/Exception.cpp
  ../castor/exception/FailedToExecutePython.cpp
  ../castor/exception/InvalidArgument.cpp
  ../castor/exception/InvalidConfigEntry.cpp
  ../castor/exception/InvalidConfiguration.cpp
  ../castor/exception/InvalidNbArguments.cpp
  ../castor/exception/Internal.cpp
  ../castor/exception/Mismatch.cpp
  ../castor/exception/NotSupported.cpp
  ../castor/exception/OutOfMemory.cpp
  ../castor/exception/QueryVolumeFailed.cpp
  ../castor/exception/Communication.cpp
  ../castor/exception/MissingOperand.cpp
  ../castor/exception/MountFailed.cpp
  ../castor/exception/NoEntry.cpp
  ../castor/exception/NoPortInRange.cpp
  ../castor/exception/NoSegmentFound.cpp
  ../castor/exception/NoValue.cpp
  ../castor/exception/PermissionDenied.cpp
  ../castor/exception/RequestCanceled.cpp
  ../castor/exception/RequestFailed.cpp
  ../castor/exception/Security.cpp
  ../castor/exception/SErrnum.cpp
  ../castor/exception/SQLError.cpp
  ../castor/exception/TimeOut.cpp
  ../castor/exception/TooBig.cpp
  ../castor/common/CastorConfiguration.cpp)

add_library (castorcommon SHARED ${COMMON_LIB_SRC_FILES})
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE Cstrerror.c Cpwd.c Cgetopt.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  message(STATUS "Adding compile option for -DCOLLECTEXTRABACKTRACEINFOS Backtrace.cpp as CMAKE_BUILD_TYPE=\"${CMAKE_BUILD_TYPE}\".")
  set_property(SOURCE ../castor/exception/Backtrace.cpp APPEND PROPERTY COMPILE_FLAGS -DCOLLECTEXTRABACKTRACEINFOS)
else(CMAKE_BUILD_TYPE STREQUAL "Debug")
  message(STATUS "CMAKE_BUILD_TYPE=\"${CMAKE_BUILD_TYPE}\": leaving Backtrace.cpp alone.")
endif(CMAKE_BUILD_TYPE STREQUAL "Debug")
target_link_libraries(castorcommon debug bfd iberty z)
CastorSetLibraryVersions (castorcommon)
target_link_libraries (castorcommon dl ${UUID_LIB})
install (TARGETS castorcommon DESTINATION ${CASTOR_DEST_LIB_DIR})

set(COMMON_MAN_PAGES
  Cgetopt
  Cglobals
  Cgrp
  Cmutex
  Cpool
  Cpwd
  Cthread
  Cuuid
  getacctent
  getacct
  getconfent
  log
  netread
  netwrite)

foreach(MAN_PAGE ${COMMON_MAN_PAGES})
  CastorInstallLibManPage(${MAN_PAGE})
endforeach(MAN_PAGE)

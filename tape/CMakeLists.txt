#
#                      tape/CMakeLists.txt
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
# Steven.Murray@cern.ch Eric.Cano@cern.ch
#
cmake_minimum_required (VERSION 2.6)

include_directories ("..")
include_directories ("../h")

# Tape source files require the BIN macro to be set
set_property (
  DIRECTORY
  APPEND PROPERTY COMPILE_DEFINITIONS BIN="/usr/bin")

# A set of eight source files for the tape library need to be copied to files to
# with usr prepended to their names.  These usr files must then be compiled
# with the -DNOTRACE option
foreach (SRC_FILE locate.c readlbl.c rwndtape.c skiptape.c tperror.c usrmsg.c
  writelbl.c wrttpmrk.c)
  set (TAPE_LIB_USR_SRC_FILES ${TAPE_LIB_USR_SRC_FILES} usr${SRC_FILE})

  add_custom_command (
    OUTPUT usr${SRC_FILE} DEPENDS ${SRC_FILE}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/${SRC_FILE} ${CMAKE_CURRENT_BINARY_DIR}/usr${SRC_FILE})

  set_property(SOURCE usr${SRC_FILE} PROPERTY COMPILE_FLAGS -DNOTRACE)
endforeach ()

set (TAPE_LIB_SRC_FILES Ctape_config.c Ctape_devinfo.c Ctape_dmpfil.c
  Ctape_errmsg.c Ctape_info.c Ctape_kill.c Ctape_label.c Ctape_mount.c
  Ctape_position.c Ctape_reserve.c Ctape_rls.c Ctape_rstatus.c Ctape_status.c
  asc2ebc.c chkdirw.c cvtden.c ebc2asc.c findpgrp.c getcompstat.c initlabel.c
  send2tpd.c usrlbl.c tplogger.c tplogger_messages.c sendscsicmd.c)

add_library (castortape SHARED ${TAPE_LIB_SRC_FILES} ${TAPE_LIB_USR_SRC_FILES})
set_target_properties (castortape PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castortape castorcommon castordlf castorvmgr)
install (TARGETS castortape DESTINATION ${CASTOR_DEST_LIB_DIR})

set (TAPED_SRC_FILES tpdaemon.c checkjobdied.c cvtden.c sendrep.c tplogit.c usrmsg.c)
add_executable(taped ${TAPED_SRC_FILES})
target_link_libraries (taped castorcommon castortape castorvdqm)

set (CONFDRIVE_SRC_FILES confdrive.c sendrep.c tplogit.c)

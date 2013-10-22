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

################################################################################
# Tape source files require the BIN macro to be set
################################################################################
set_property (
  DIRECTORY
  APPEND PROPERTY COMPILE_DEFINITIONS BIN="/usr/bin")

################################################################################
# A set of eight source files for the tape library need to be copied to files to
# with usr prepended to their names.  These usr files must then be compiled
# with the -DNOTRACE option
################################################################################
foreach (SRC_FILE locate.c readlbl.c rwndtape.c skiptape.c tperror.c usrmsg.c
  writelbl.c wrttpmrk.c)
  set (TAPE_LIB_USR_SRC_FILES ${TAPE_LIB_USR_SRC_FILES} usr${SRC_FILE})

  add_custom_command (
    OUTPUT usr${SRC_FILE} DEPENDS ${SRC_FILE}
    COMMAND ${CMAKE_COMMAND} -E copy
      ${CMAKE_CURRENT_SOURCE_DIR}/${SRC_FILE}
      ${CMAKE_CURRENT_BINARY_DIR}/usr${SRC_FILE})

  set_property(SOURCE usr${SRC_FILE} PROPERTY COMPILE_FLAGS -DNOTRACE)
endforeach ()

################################################################################
# Rules to build libcastortape.so
################################################################################
set (TAPE_LIB_SRC_FILES Ctape_config.c Ctape_devinfo.c Ctape_dmpfil.c
  Ctape_errmsg.c Ctape_info.c Ctape_kill.c Ctape_label.c Ctape_mount.c
  Ctape_position.c Ctape_reserve.c Ctape_rls.c Ctape_rstatus.c Ctape_status.c
  asc2ebc.c chkdirw.c cvtden.c ebc2asc.c findpgrp.c getcompstat.c initlabel.c
  send2tpd.c usrlbl.c tplogger.c tplogger_messages.c sendscsicmd.c)
add_library (castortape SHARED ${TAPE_LIB_SRC_FILES} ${TAPE_LIB_USR_SRC_FILES})
set_target_properties (castortape PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castortape castorcommon castordlf castorvmgr)
install (TARGETS castortape DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to install the man pages of libcastortape.so
################################################################################
set (TAPE_LIB_MAN_PAGES Ctape_dmpfil Ctape_info Ctape_kill Ctape_label
  Ctape_mount Ctape_position Ctape_reserve Ctape_rls Ctape_rstatus Ctape_status
  getcompstat readlbl rwndtape skiptape usrlbl wrttpmrk)
foreach (MAN_PAGE ${TAPE_LIB_MAN_PAGES})
  install (FILES ${MAN_PAGE}.man DESTINATION ${CASTOR_DEST_MAN_DIR}/man3
    RENAME ${MAN_PAGE}.3castor)
endforeach ()

################################################################################
# Rules to build tpconfig and install its man page
################################################################################
add_executable (tpconfig tpconfig.c)
target_link_libraries (tpconfig castorcommon castortape)
install (FILES tpconfig.man DESTINATION ${CASTOR_DEST_MAN_DIR}/man1
  RENAME tpconfig.1castor)

################################################################################
# Rules to build tpstat and install its man page
################################################################################
add_executable (tpstat tpstat.c)
target_link_libraries (tpstat castortape)
install (FILES tpstat.man DESTINATION ${CASTOR_DEST_MAN_DIR}/man1
  RENAME tpstat.1castor)

################################################################################
# Rules to build taped and install its man page
################################################################################
set (TAPED_SRC_FILES tpdaemon.c checkjobdied.c cvtden.c sendrep.c tplogit.c
  usrmsg.c)
add_executable (taped ${TAPED_SRC_FILES})
target_link_libraries (taped castorcommon castortape castorvdqm)
install (TARGETS taped DESTINATION ${CASTOR_DEST_BIN_DIR})
install (FILES taped.man DESTINATION ${CASTOR_DEST_MAN_DIR}/man8
  RENAME taped.8castor)

################################################################################
# Rules to build confdrive
################################################################################
set (CONFDRIVE_SRC_FILES confdrive.c sendrep.c tplogit.c)
add_executable(confdrive ${CONFDRIVE_SRC_FILES})
target_link_libraries(confdrive castorcommon castortape castorvdqm)
install (TARGETS confdrive DESTINATION ${CASTOR_DEST_BIN_DIR})

################################################################################
# Rules to build posovl
################################################################################
set (POSOVL_SRC_FILES posovl.c buildhdrlbl.c builduhl.c buildvollbl.c
  ebc2asc.c getdrvstatus.c inquiry.c locate.c posittape.c readlbl.c rwndtape.c
  send2tpd.c sendrep.c skiptape.c tperror.c tplogit.c usrmsg.c)
add_executable(posovl ${POSOVL_SRC_FILES})
target_link_libraries(posovl castorcommon castortape)
install (TARGETS posovl DESTINATION ${CASTOR_DEST_BIN_DIR})

################################################################################
# Rules to build tplabel and install its man page
################################################################################
add_executable(tplabel tplabel.c)
target_link_libraries(tplabel castorcommon castortape)
install (TARGETS tplabel DESTINATION ${CASTOR_DEST_BIN_DIR})
install (FILES tplabel.man DESTINATION ${CASTOR_DEST_MAN_DIR}/man1
  RENAME tplabel.1castor)

################################################################################
# Rules to build mounttape-nostk
################################################################################
set (MOUNTTAPE_NOSTK_SRC_FILES mounttape.c asc2ebc.c buildhdrlbl.c
  buildvollbl.c ebc2asc.c getdrvstatus.c inquiry.c rbtsubr.c readlbl.c
  rwndtape.c send2tpd.c sendrep.c setCompression.c skiptape.c tperror.c
  tplogit.c unldtape.c usrmsg.c writelbl.c wrttpmrk.c mircheck.c)
add_executable(mounttape-nostk ${MOUNTTAPE_NOSTK_SRC_FILES})
target_link_libraries(mounttape-nostk castorcommon castorrmc castortape
  castorvdqm)
install (TARGETS mounttape-nostk DESTINATION ${CASTOR_DEST_BIN_DIR})

################################################################################
# Rules to build rlstape-nostk
################################################################################
set (RLSTAPE_NOSTK_SRC_FILES rlstape.c getdrvstatus.c rbtsubr.c send2tpd.c
  sendrep.c tperror.c tplogit.c usrmsg.c unldtape.c tapealertcheck.c)
add_executable(rlstape-nostk ${RLSTAPE_NOSTK_SRC_FILES})
target_link_libraries(rlstape-nostk castorcommon castorrmc castortape
  castorvdqm)
install (TARGETS rlstape-nostk DESTINATION ${CASTOR_DEST_BIN_DIR})

################################################################################
# Try to find the four STK client libaries: libapi.so, libutl.so, libipc.so and
# libcl.so
################################################################################
find_library (STK_API_LIB api PATHS /usr/lib64/CDK NO_DEFAULT_PATH)
if (STK_API_LIB)
  message(STATUS "Found the STK api library: ${STK_API_LIB}")
endif (STK_API_LIB)
find_library (STK_UTL_LIB utl PATHS /usr/lib64/CDK NO_DEFAULT_PATH)
if (STK_UTL_LIB)
  message(STATUS "Found the STK utl library: ${STK_UTL_LIB}")
endif (STK_UTL_LIB)
find_library (STK_IPC_LIB ipc PATHS /usr/lib64/CDK NO_DEFAULT_PATH)
if (STK_IPC_LIB)
  message(STATUS "Found the STK ipc library: ${STK_IPC_LIB}")
endif (STK_IPC_LIB)
find_library (STK_CL_LIB cl PATHS /usr/lib64/CDK NO_DEFAULT_PATH)
if (STK_CL_LIB)
  message(STATUS "Found the STK cl library: ${STK_CL_LIB}")
endif (STK_CL_LIB)

################################################################################
# Add the mounttape and rlstape targets if all four STK client libaries
# (libapi.so, libutl.so, libipc.so and libcl.so) have been found
################################################################################
if (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
  message(STATUS "Found all four STK libraries")

  ##############################################################################
  # Create the mounttape-stk.c and rbtsubr-stk.c source files from mounttape.c
  # and rbtsubr.c respectively and give these teo new source files the following
  # compilation flags: -DCDK -DLINUX -I/usr/include/CDK
  ##############################################################################
  foreach(SRC_FILE mounttape rbtsubr)
    add_custom_command (
      OUTPUT ${SRC_FILE}-stk.c DEPENDS ${SRC_FILE}.c
      COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/${SRC_FILE}.c
        ${CMAKE_CURRENT_BINARY_DIR}/${SRC_FILE}-stk.c)

    set_property(SOURCE ${SRC_FILE}-stk.c
      PROPERTY COMPILE_FLAGS "-DCDK -DLINUX -I/usr/include/CDK")
  endforeach ()

  ##############################################################################
  # Rules to build mounttape
  ##############################################################################
  message(STATUS "Adding the mounttape target")
  set (MOUNTTAPE_SRC_FILES mounttape-stk.c asc2ebc.c buildhdrlbl.c
    buildvollbl.c ebc2asc.c getdrvstatus.c inquiry.c rbtsubr-stk.c readlbl.c
    rwndtape.c send2tpd.c sendrep.c setCompression.c skiptape.c tperror.c
    tplogit.c unldtape.c usrmsg.c writelbl.c wrttpmrk.c mircheck.c)
  add_executable(mounttape ${MOUNTTAPE_SRC_FILES})
  target_link_libraries(mounttape castorcommon castorrmc castortape castorvdqm
    ${STK_API_LIB} ${STK_UTL_LIB} ${STK_IPC_LIB} ${STK_CL_LIB})
  install (TARGETS mounttape DESTINATION ${CASTOR_DEST_BIN_DIR})

  ##############################################################################
  # Rules to build rlstape
  ##############################################################################
  message(STATUS "Adding the rlstape target")
  set (RLSTAPE_SRC_FILES rlstape.c getdrvstatus.c rbtsubr-stk.c send2tpd.c
    sendrep.c tperror.c tplogit.c usrmsg.c unldtape.c tapealertcheck.c)
  add_executable(rlstape ${RLSTAPE_SRC_FILES})
  target_link_libraries(rlstape castorcommon castorrmc castortape
    castorvdqm ${STK_API_LIB} ${STK_UTL_LIB} ${STK_IPC_LIB} ${STK_CL_LIB})
  install (TARGETS rlstape DESTINATION ${CASTOR_DEST_BIN_DIR})
endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)

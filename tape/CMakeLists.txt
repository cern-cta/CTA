cmake_minimum_required (VERSION 2.6)

include_directories ("..")
include_directories ("../h")

#set (CMAKE_C_FLAGS "-O0 -g -pthread -Wall -Wextra -Werror -Wno-unused-parameter")
set_property (
  DIRECTORY
  APPEND PROPERTY COMPILE_DEFINITIONS BIN="/usr/bin")

# A set of eight source files for the tape library need to be copied to files to
# with usr prepended to their names.  These usr... source files must then have
# -DNOTRACE added to their compilation flags
foreach (SRC_FILE locate.c readlbl.c rwndtape.c skiptape.c tperror.c usrmsg.c
  writelbl.c wrttpmrk.c)
  set (TAPE_LIB_USR_SRC_FILES ${TAPE_LIB_USR_SRC_FILES} usr${SRC_FILE})

  add_custom_command (
    OUTPUT usr${SRC_FILE} DEPENDS ${SRC_FILE}
    COMMAND ${CMAKE_COMMAND} -E copy ${SRC_FILE} usr${SRC_FILE})

  set_property(SOURCE usr${SRC_FILE} PROPERTY COMPILE_FLAGS -DNOTRACE)
endforeach ()

set (TAPE_LIB_SRC_FILES Ctape_config.c Ctape_devinfo.c Ctape_dmpfil.c
  Ctape_errmsg.c Ctape_info.c Ctape_kill.c Ctape_label.c Ctape_mount.c
  Ctape_position.c Ctape_reserve.c Ctape_rls.c Ctape_rstatus.c Ctape_status.c
  asc2ebc.c chkdirw.c cvtden.c ebc2asc.c findpgrp.c getcompstat.c initlabel.c
  send2tpd.c usrlbl.c tplogger.c tplogger_messages.c sendscsicmd.c)

add_library (castortape SHARED ${TAPE_LIB_SRC_FILES} ${TAPE_LIB_USR_SRC_FILES})

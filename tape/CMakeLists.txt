cmake_minimum_required (VERSION 2.6)

include_directories ("..")
include_directories ("../h")

add_custom_command (OUTPUT usrlocate.c DEPENDS locate.c
  COMMAND ${CMAKE_COMMAND} -E copy locate.c usrlocate.c)
add_custom_command (OUTPUT usrreadlbl.c DEPENDS readlbl.c
  COMMAND ${CMAKE_COMMAND} -E copy readlbl.c usrreadlbl.c)
add_custom_command (OUTPUT usrrwndtape.c DEPENDS rwndtape.c
  COMMAND ${CMAKE_COMMAND} -E copy rwndtape.c usrrwndtape.c)
add_custom_command (OUTPUT usrskiptape.c DEPENDS skiptape.c
  COMMAND ${CMAKE_COMMAND} -E copy skiptape.c usrskiptape.c)
add_custom_command (OUTPUT usrtperror.c DEPENDS tperror.c
  COMMAND ${CMAKE_COMMAND} -E copy tperror.c usrtperror.c)
add_custom_command (OUTPUT usrusrmsg.c DEPENDS usrmsg.c
  COMMAND ${CMAKE_COMMAND} -E copy usrmsg.c usrusrmsg.c)
add_custom_command (OUTPUT usrwritelbl.c DEPENDS writelbl.c
  COMMAND ${CMAKE_COMMAND} -E copy writelbl.c usrwritelbl.c)
add_custom_command (OUTPUT usrwrttpmrk.c DEPENDS wrttpmrk.c
  COMMAND ${CMAKE_COMMAND} -E copy wrttpmrk.c usrwrttpmrk.c)

set_property(SOURCE usrlocate.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrreadlbl.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrrwndtape.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrskiptape.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrtperror.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrusrmsg.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrwritelbl.c PROPERTY COMPILE_FLAGS -DNOTRACE)
set_property(SOURCE usrwrttpmrk.c PROPERTY COMPILE_FLAGS -DNOTRACE)

set (TAPE_LIB_SRC_FILES Ctape_config.c Ctape_devinfo.c Ctape_dmpfil.c
Ctape_errmsg.c Ctape_info.c Ctape_kill.c Ctape_label.c Ctape_mount.c
Ctape_position.c Ctape_reserve.c Ctape_rls.c Ctape_rstatus.c Ctape_status.c
asc2ebc.c chkdirw.c cvtden.c ebc2asc.c findpgrp.c getcompstat.c initlabel.c
send2tpd.c usrlbl.c usrlocate.c usrreadlbl.c usrrwndtape.c usrskiptape.c
usrtperror.c usrusrmsg.c usrwritelbl.c usrwrttpmrk.c tplogger.c
tplogger_messages.c sendscsicmd.c)

add_library (castortape SHARED ${TAPE_LIB_SRC_FILES})

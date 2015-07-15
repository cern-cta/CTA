#
#                      castor/CMakeLists.txt
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

install(FILES shift.h
  DESTINATION ${CASTOR_DEST_INCLUDE_DIR}
  PERMISSIONS ${CASTOR_HEADER_PERMS})

set(SHIFT_SUBHEADERS 
  Castor_limits.h
  Cgetopt.h
  Cglobals.h
  Cgrp.h
  Cinit.h
  Cmutex.h
  Cnetdb.h
  Cns.h
  Cns_api.h
  Cns_constants.h
  Cns_server.h
  Cns_struct.h
  Cpool_api.h
  Cpwd.h
  Cregexp.h
  Cregexp_magic.h
  Csec.h
  Csec_api.h
  Csec_common.h
  Csec_constants.h
  Csec_plugin.h
  Csemaphore.h
  Csnprintf.h
  Cstrerror.h
  Ctape.h
  Ctape_api.h
  Ctape_constants.h
  Cthread_api.h
  Cthread_flags.h
  Cthread_typedef.h
  Cupv.h
  Cupv_api.h
  Cupv_constants.h
  Cupv_server.h
  Cupv_struct.h
  Cupv_util.h
  Cuuid.h
  RfioTURL.h
  common.h
  dlf_api.h
  getacct.h
  getacctent.h
  getconfent.h
  log.h
  marshall.h
  md5.h
  net.h
  osdep.h
  ${CMAKE_BINARY_DIR}/h/patchlevel.h
  rfcntl.h
  rfio.h
  rfio_api.h
  rfio_constants.h
  rfio_errno.h
  rfio_lcastorfdt.h
  scsictl.h
  serrno.h
  stage_api.h
  stage_constants.h
  stage_limits.h
  stager_api.h
  stager_client_api.h
  stager_constants.h
  stager_errmsg.h
  stager_mapper.h
  stager_mover_api.h
  stager_util.h
  trace.h
  u64subr.h
  util.h
  vdqm.h
  vdqm_api.h
  vdqm_constants.h
  vmgr.h
  vmgr_api.h
  vmgr_constants.h
  vmgr_server.h
  vmgr_struct.h
  ypgetacctent.h)

install(FILES ${SHIFT_SUBHEADERS}
  DESTINATION ${CASTOR_DEST_INCLUDE_DIR}/shift
  PERMISSIONS ${CASTOR_HEADER_PERMS})

CastorInstallFileManPage(Castor_limits)
CastorInstallLibManPage(serrno)

if (${COMPILE_SERVER} STREQUAL "1")
  ##############################################################################
  # Installation of header files for package castor-build-headers
  ##############################################################################
  
  set(BUILD_HEADERS
    # Required by xrootd-xcastor2fs and castor-srmv2
    Castor_limits.h
    Cns_api.h
    Cns_constants.h
    Cns_struct.h
    Cthread_api.h
    Cthread_flags.h
    Cuuid.h
    dlf_api.h
    net.h
    osdep.h
    serrno.h
    stager_api.h
    stager_client_api_common.hpp
    stager_client_api.h
    stager_errmsg.h
  
    # Required by castor-srmv2
    Cgetopt.h
    Cglobals.h
    Cgrp.h
    Cnetdb.h
    common.h
    Cpool_api.h
    Cpwd.h
    Ctape_api.h
    Ctape_constants.h
    getacctent.h
    getacct.h
    getconfent.h
    log.h
    ${CMAKE_BINARY_DIR}/h/patchlevel.h
    rfio_api.h
    rfio_constants.h
    rfio_errno.h
    stager_mapper.h
    trace.h
    u64subr.h
    vmgr_api.h
    vmgr_constants.h
    vmgr_struct.h
    ypgetacctent.h)
  
  install(FILES ${BUILD_HEADERS}
    DESTINATION ${CASTOR_DEST_C_HEADERS_DIR}
    PERMISSIONS ${CASTOR_HEADER_PERMS})
endif (${COMPILE_SERVER} STREQUAL "1")

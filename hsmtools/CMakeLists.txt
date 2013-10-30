#                    hsmtools/CMakeLists.txt
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

################################################################################
# Rules to build and install hsmtools
################################################################################

# We rely here on a hacked version of rfcp
add_custom_command (
  OUTPUT rfcpupd.c
  DEPENDS ${CMAKE_SOURCE_DIR}/rfio/rfcp.c
  COMMAND
    cp ${CMAKE_SOURCE_DIR}/rfio/rfcp.c ${CMAKE_CURRENT_BINARY_DIR}/rfcpupd.c
  COMMAND
    sed -i 's/| *O_TRUNC//' ${CMAKE_CURRENT_BINARY_DIR}/rfcpupd.c
  COMMAND
    sed -i 's/O_WRONLY/O_RDWR|O_APPEND/' ${CMAKE_CURRENT_BINARY_DIR}/rfcpupd.c)

# The c programs
set(HSM_SINGLE_FILE_C_TARGETS
  adler32
  c2probe
  reclaim
  rfcpupd)
foreach(TOOL ${HSM_SINGLE_FILE_C_TARGETS})
  add_executable(${TOOL} ${TOOL}.c)
  target_link_libraries (${TOOL} castorcommon castorns castorclient castorrfio castorvmgr)
endforeach(TOOL)
target_link_libraries (adler32 -lz)

# The c++ programs
set(HSM_SINGLE_FILE_CPP_TARGETS
  stager_actualget
  stager_actualput)
foreach(TOOL ${HSM_SINGLE_FILE_CPP_TARGETS})
  add_executable(${TOOL} ${TOOL}.cpp)
  target_link_libraries (${TOOL} castorcommon castorns castorclient castorrfio castorvmgr)
endforeach(TOOL)

# Common installation
install(TARGETS ${HSM_SINGLE_FILE_C_TARGETS} ${HSM_SINGLE_FILE_CPP_TARGETS}
  DESTINATION ${CASTOR_DEST_BIN_DIR})

# The scripts
set(HSM_SCRIPTS
  repack

  enterfileclass 
  deletefileclass
  printfileclass
  
  entersvcclass
  modifysvcclass
  deletesvcclass
  printsvcclass
  
  enterdiskpool
  deletediskpool
  printdiskpool
  
  enterrecallgroup
  modifyrecallgroup
  deleterecallgroup
  printrecallgroup
  
  enterrecalluser
  modifyrecalluser
  deleterecalluser
  printrecalluser
  
  entertapepool
  modifytapepool
  deletetapepool
  printtapepool
  
  entermigrationroute
  modifymigrationroute
  deletemigrationroute
  printmigrationroute
  
  modifydiskserver
  deletediskserver
  printdiskserver
  
  modifydbconfig
  printdbconfig
  
  fixFileSize
  deletediskcopy
  diskserver_qry
  draindiskserver
  migratenewcopy
  vdqmlistpriority
  vdqmlistrequest
  vdqmsetpriority
  vdqmdeletepriority
  
  printmigrationstatus
  printrecallstatus
  printrecallfilestatus
  printrecalltapequeue
  printrecalluserqueue
)
install(FILES ${HSM_SCRIPTS}
  DESTINATION ${CASTOR_DEST_BIN_DIR}
  PERMISSIONS ${CASTOR_SCRIPT_PERMS})

# The libraries
install(FILES castor_tools.py dlf.py
  DESTINATION ${CASTOR_DEST_PYTHON_LIBDIR}
  PERMISSIONS ${CASTOR_PYTHON_LIB_PERMS})
install(FILES castor_tools.pm
  DESTINATION ${CASTOR_DEST_PERL_LIBDIR}
  PERMISSIONS ${CASTOR_SCRIPT_PERMS})
  

################################################################################
# Rules to install hsmtools's man page(s) and init scripts
################################################################################

set(HSM_MAN_PAGES
  adler32
  adminMultiInstance
  c2probe
  deletediskcopy
  deletediskpool
  deletediskserver
  deletefileclass
  deletemigrationroute
  deleterecallgroup
  deleterecalluser
  deletesvcclass
  deletetapepool
  diskserver_qry
  draindiskserver
  enterdiskpool
  enterfileclass
  entermigrationroute
  enterrecallgroup
  enterrecalluser
  entersvcclass
  entertapepool
  migratenewcopy
  modifydbconfig
  modifydiskserver
  modifymigrationroute
  modifyrecallgroup
  modifyrecalluser
  modifysvcclass
  modifytapepool
  printdbconfig
  printdiskpool
  printdiskserver
  printfileclass
  printmigrationroute
  printmigrationstatus
  printrecallfilestatus
  printrecallgroup
  printrecallstatus
  printrecalltapequeue
  printrecalluser
  printrecalluserqueue
  printsvcclass
  printtapepool
  reclaim
  repack
  vdqmdeletepriority
  vdqmlistpriority
  vdqmlistrequest
  vdqmsetpriority)

foreach(PAGE ${HSM_MAN_PAGES})
  CastorInstallExeManPage(${PAGE})
endforeach(PAGE)

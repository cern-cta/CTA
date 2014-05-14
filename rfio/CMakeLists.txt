#
#                      rfio/CMakeLists.txt
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

################################################################################
# Rules to build and install libcastorrfio.so
################################################################################
set_property(SOURCE
  fstat64.c
  lseek64.c
  open64.c
  preseek64.c
  read64.c
  stream64.c
  write64.c
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
set (RFIO_LIB_SRC_FILES access.c chdir.c checkkey.c chmod.c chown.c
  close.c closedir.c connect.c error.c fchmod.c fchown.c fclose.c feof.c
  ferror.c fflush.c fileno.c fio.c fopen.c fread.c fseek.c fstat.c ftell.c
  fwrite.c getc.c getcwd.c lseek.c lstat.c lun2fn.c misc.c mkdir.c
  mstat.c msymlink.c munlink.c open.c opendir.c parse.c pclose.c popen.c
  pread.c preseek.c pwrite.c read.c readdir.c readlink.c rename.c rewinddir.c
  rfio_HsmIf.c rfio_rdirfdt.c rfio_rfilefdt.c rfstatfs.c rmdir.c setopt.c stat.c
  statfs.c stream.c symlink.c unlink.c write.c xyclose.c xyopen.c
  xyread.c xywrite.c fopen64.c fseeko64.c fstat64.c ftello64.c
  lseek64.c open64.c preseek64.c read64.c stream64.c write64.c RfioTURL.c)
add_library (castorrfio SHARED ${RFIO_LIB_SRC_FILES})
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE connect.c error.c mstat.c msymlink.c munlink.c parse.c 
rfio_HsmIf.c stream.c RfioTURL.c
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
CastorSetLibraryVersions (castorrfio)
set_property (TARGET castorrfio
  APPEND PROPERTY COMPILE_DEFINITIONS USE_XFSPREALLOC)
target_link_libraries (castorrfio castorns castorclient)
install (TARGETS castorrfio DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to install the man pages
################################################################################
set (RFIO_MAN_FILES rfio_fileno rfio_read rfio_lstat rfio_readlink rfio_chown
  rfio_mstat rfio_rewinddir rfio_fseeko64 rfio_rename rfio_preseek rfio_ftello64
  rfio_write rfio_unlink rfio_fseek rfio_close rfio_msymlink rfio_fread
  rfio_mstat64 rfio_open64 rfio_lseek rfio_closedir rfio_ftell rfio_stat
  rfio_perror rfio_stat64 rfio_readdir rfio_opendir rfio_fwrite rfio_rmdir
  rfio_lstat64 rfio_fchmod rfio_fflush rfio_preseek64 rfio_pclose
  rfio_mkdir rfio_access rfio_symlink rfio_munlink rfio_open rfio_statfs
  rfio_fstat64 rfio_fstat rfio_feof rfio_popen rfio_serror
  rfio_fclose rfio_fopen64 rfio_lseek64 rfio_ferror rfio_chmod rfio_fopen
  rfioreadopt rfiosetopt)
foreach(MAN ${RFIO_MAN_FILES})
  CastorInstallLibManPage(${MAN})
endforeach(MAN)

################################################################################
# Rules to build and install rfio's CLI commands (and man pages)
################################################################################

set (RFIO_CLI_CLIENTS rfcat rfchmod rfcp rfdir rfmkdir rfrename rfrm)
set (RFIO_CLI_CLIENTS_NO_MANPAGE  rfstat)
foreach(CLT ${RFIO_CLI_CLIENTS} ${RFIO_CLI_CLIENTS_NO_MANPAGE})
  add_executable(${CLT} ${CLT}.c)
  target_link_libraries(${CLT} castorcommon castorns castordlf castorsecurity
      castorclient castorrfio)
endforeach(CLT)
install(TARGETS ${RFIO_CLI_CLIENTS} ${RFIO_CLI_CLIENTS_NO_MANPAGE}
  DESTINATION ${CASTOR_DEST_BIN_DIR})

foreach(CLT ${RFIO_CLI_CLIENTS})
  CastorInstallExeManPage(${CLT})
endforeach(CLT)

if (${COMPILE_SERVER} STREQUAL "1")
  ##############################################################################
  # Rules to build and install the server rfiod, man pages and config files
  ##############################################################################
  find_package (ceph REQUIRED)
  include_directories(${RADOS_INCLUDE_DIR})
  set_property(SOURCE rfio_calls.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
  set_property (SOURCE rfio_call64.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
  set(RFIOD_SRC_FILES Csemaphore.c rfio_serv.c rfio_calls.c rfio_call64.c 
                      rfio_callhandlers.c checkkey.c fio.c rfstatfs.c rfio_localio.cpp
                      rfio_alignedbuf.c rfio_xfsprealloc.c)
  add_executable(rfiod ${RFIOD_SRC_FILES})
  # Relax compilation of old code full of pointer casting. Funnily enough,
  # the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
  target_link_libraries(rfiod castorcommon castorns castordlf castorsecurity castorclient castorrfio ${RADOS_LIBS})
  set_target_properties(rfiod PROPERTIES LINK_FLAGS "-lcrypto -ldl -lz")
  install(TARGETS rfiod DESTINATION ${CASTOR_DEST_BIN_DIR})
  
  CastorInstallExeManPage(rfiod)
  CastorInstallInitScript(rfiod)
  CastorInstallSysconfigExample(rfiod)
  CastorInstallLogrotate(castor-rfio-server)
  CastorInstallDir(/var/lib/rfiod)
endif (${COMPILE_SERVER} STREQUAL "1")

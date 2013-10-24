#
#                      ns/CMakeLists.txt
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
# Rules to build and install libcastorns.so
################################################################################
set (NS_LIB_SRC_FILES
  Cns_aborttrans.c
  Cns_access.c
  Cns_apiinit.c
  Cns_auth.c
  Cns_bulkexist.c
  Cns_chclass.c
  Cns_chdir.c
  Cns_chmod.c
  Cns_chown.c
  Cns_closex.c
  Cns_closedir.c
  Cns_creat.c
  Cns_delcomment.c
  Cns_deleteclass.c
  Cns_dropsegs.c
  Cns_delsegbycopyno.c
  Cns_du.c
  Cns_endsess.c
  Cns_endtrans.c
  Cns_enterclass.c
  Cns_errmsg.c
  Cns_getacl.c
  Cns_getcomment.c
  Cns_getcwd.c
  Cns_getlinks.c
  Cns_getpath.c
  Cns_getsegattrs.c
  Cns_lastfseq.c
  Cns_listclass.c
  Cns_listtape.c
  Cns_listlinks.c
  Cns_mkdir.c
  Cns_modifyclass.c
  Cns_openx.c
  Cns_opendir.c
  Cns_ping.c
  Cns_queryclass.c
  Cns_readdir.c
  Cns_readdirc.c
  Cns_readdirg.c
  Cns_readdirx.c
  Cns_readdirxc.c
  Cns_readdirxt.c
  Cns_readlink.c
  Cns_rename.c
  Cns_rewinddir.c
  Cns_replaceseg.c
  Cns_rmdir.c
  Cns_selectsrvr.c
  Cns_setacl.c
  Cns_setatime.c
  Cns_setcomment.c
  Cns_setfsize.c
  Cns_startsess.c
  Cns_starttrans.c
  Cns_stat.c
  Cns_symlink.c
  Cns_tapesum.c
  Cns_umask.c
  Cns_unlink.c
  Cns_unlinkbyvid.c
  Cns_updateseg_checksum.c
  Cns_updateseg_status.c
  Cns_updatefile_checksum.c
  Cns_utime.c
  send2nsd.c)
add_library (castorns SHARED ${NS_LIB_SRC_FILES})
set_target_properties (castorns
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castorns castorsecurity)
install (TARGETS castorns DESTINATION ${CASTOR_DEST_LIB_DIR})

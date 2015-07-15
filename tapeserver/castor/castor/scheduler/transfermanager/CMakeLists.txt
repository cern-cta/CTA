#            castor/scheduler/transfermanager/CMakeLists.txt
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
# Rules to install transfermanager's scripts
################################################################################

set(TRANSFERMANAGER_SCRIPTS
  serverqueue.py
  dispatcher.py
  aborter.py
  synchronizer.py
  suicider.py
  reportmanager.py
  diskserverlistcache.py
  modifydiskservers.py
  transfermanagerdlf.py)
install(FILES ${TRANSFERMANAGER_SCRIPTS}
  DESTINATION ${CASTOR_DEST_PYTHON_LIBDIR}
  PERMISSIONS ${CASTOR_SCRIPT_PERMS})
install(FILES transfermanagerd
  DESTINATION ${CASTOR_DEST_BIN_DIR}
  PERMISSIONS ${CASTOR_SCRIPT_PERMS})

################################################################################
# Rules to install transfermanager's man page(s) and init scripts
################################################################################

CastorInstallAdmManPage(transfermanagerd)
CastorInstallInitScript(transfermanagerd)
CastorInstallSysconfigExample(transfermanagerd)
CastorInstallLogrotate(castor-transfer-manager)

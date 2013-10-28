#
#                      tapebridge/CMakeLists.txt
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
# Rules to build and install libcastortapebridge.so
################################################################################
set (TAPEBRIDGE_LIB_SRC_FILES
  tapebridge_marshall.c
  tapebridge_sendTapeBridgeClientInfo2Ack.c
  tapebridge_sendTapeBridgeFlushedToTape.c
  tapebridge_recvTapeBridgeFlushedToTapeAck.c
  tapebridge_tapeFlushModeToStr.c)
add_library (castortapebridge SHARED ${TAPEBRIDGE_LIB_SRC_FILES})
set_target_properties (castortapebridge
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castortapebridge castorcommon)
install (TARGETS castortapebridge DESTINATION ${CASTOR_DEST_LIB_DIR})

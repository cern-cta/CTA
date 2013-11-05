#
#                      security/CMakeLists.txt
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
# Rules to build and install libcastorsecurity.so
################################################################################
set (SECURITY_LIB_SRC_FILES Csec_api_loader.c Csec_protocol_policy.c
  Csec_api.c Csec_errmsg.c Csec_apiinit.c Csec_common.c)
add_library (castorsecurity SHARED ${SECURITY_LIB_SRC_FILES})
set_target_properties (castorsecurity
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION}.${MINOR_CASTOR_VERSION})
#target_link_libraries (castorsecurity castorcommon)
install (TARGETS castorsecurity DESTINATION ${CASTOR_DEST_LIB_DIR})

CastorInstallLibManPage(Csec_api)

################################################################################
# Rules to build and install plugins
################################################################################

# Detect the kerberos environment
FIND_PROGRAM(KRB5_CONFIG
  NAMES krb5-config
  PATHS "/usr/kerberos/bin;/usr/bin")
string(REGEX REPLACE "\n" "" KRB5_CONFIG ${KRB5_CONFIG})
message(STATUS "KRB5_CONFIG = '${KRB5_CONFIG}'")
execute_process(
  COMMAND ${KRB5_CONFIG} gssapi --cflags
  OUTPUT_VARIABLE KRB5_CFLAGS)
string(REGEX REPLACE "\n" "" KRB5_CFLAGS "${KRB5_CFLAGS}")
message(STATUS "KRB5_CFLAGS = '${KRB5_CFLAGS}'")
execute_process(
  COMMAND ${KRB5_CONFIG} gssapi --libs
  OUTPUT_VARIABLE KRB5_LIBS)
string(REGEX REPLACE "\n" "" KRB5_LIBS "${KRB5_LIBS}")
message(STATUS "KRB5_LIBS = '${KRB5_LIBS}'")

# Compile the plugin
set(SECURITY_PLUGIN_KRB5
  Csec_plugin_GSS.c
  Csec_plugin_GSS_mapper.c)
add_library(castorsec_plugin_KRB5 SHARED ${SECURITY_PLUGIN_KRB5})
set_property(SOURCE ${SECURITY_PLUGIN_KRB5}
  PROPERTY COMPILE_FLAGS -DKRB5 ${KRB5_CFLAGS})
target_link_libraries (castorsec_plugin_KRB5 castorcommon ${KRB5_LIBS})
set_target_properties (castorsec_plugin_KRB5
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION}.${MINOR_CASTOR_VERSION})
install (TARGETS castorsec_plugin_KRB5 DESTINATION ${CASTOR_DEST_LIB_DIR})

add_library(castorsec_plugin_ID SHARED Csec_plugin_ID.c)
target_link_libraries (castorsec_plugin_ID castorcommon -lc -ldl)
set_target_properties (castorsec_plugin_ID
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION}.${MINOR_CASTOR_VERSION})
install (TARGETS castorsec_plugin_ID DESTINATION ${CASTOR_DEST_LIB_DIR})


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
CastorSetLibraryVersions (castorsecurity)
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
add_library(castorsec_krb5plugin SHARED ${SECURITY_PLUGIN_KRB5})

# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE ${SECURITY_PLUGIN_KRB5}
  PROPERTY COMPILE_FLAGS "-DKRB5 ${KRB5_CFLAGS} -fno-strict-aliasing")
# avoid deprecation warnings on MAC for gss_api
if (APPLE)
  set_property(SOURCE ${SECURITY_PLUGIN_KRB5} APPEND_STRING
    PROPERTY COMPILE_FLAGS " -Wno-deprecated-declarations")
endif (APPLE)
target_link_libraries (castorsec_krb5plugin castorcommon ${KRB5_LIBS})
CastorSetLibraryVersions (castorsec_krb5plugin)
install (TARGETS castorsec_krb5plugin DESTINATION ${CASTOR_DEST_LIB_DIR})


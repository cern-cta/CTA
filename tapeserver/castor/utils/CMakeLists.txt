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
# @author Castor Dev team, castor-dev@cern.ch
#
cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver)

set (UTILS_LIB_SRC_FILES
  ../common/CastorConfiguration.cpp)

add_library (ctautils SHARED ${UTILS_LIB_SRC_FILES})
set_property(TARGET ctautils PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctautils PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctautils)

install(TARGETS ctautils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

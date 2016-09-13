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

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

################################################################################
# Rules to build the reactor code that is common to both rmcd and tapeserverd
################################################################################
set (REACTOR_SRC_FILES
  ZMQPollEventHandler.cpp
  ZMQReactor.cpp)
add_library (ctatapereactor ${REACTOR_SRC_FILES})
target_link_libraries (ctatapereactor)

add_library (ctatapereactorutils SHARED
  DummyZMQReactor.cpp)

install(TARGETS ctatapereactorutils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library (ctatapereactorunittests SHARED
  ZMQReactorTest.cpp)

install(TARGETS ctatapereactorunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctatapereactorunittests
  ctatapereactor
  ctatapereactorutils)

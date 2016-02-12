#                      cmake/Findgmock.cmake
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
# castor-dev@cern.ch
#

#
# We define here 2 main variables : GMOCK_LINK and GMOCK_SRC
#   for slc6, GMOCK_SRC will be empty
#   for cc7 and later it's GMOCK_LINK that will be empty
# If not empty, GMOCK_LIB is pointing to the gmock library
# If not empty, GMOCK_SRC will contain the name of the gmock-all.cc
# to be used (something like "/usr/src/gmock/gmock-all.cc")
# Executables should use GMOCK_LIB AND GMOCK_SRC so that
# compilation will work on all platforms
#

# deal with the gmock-all.cc file
set (GMOCK_ALL_FILE "/usr/src/gmock/gmock-all.cc")
IF (EXISTS ${GMOCK_ALL_FILE})
  set (GMOCK_SRC ${GMOCK_ALL_FILE} CACHE FILEPATH "Location of gmock-all.cc or empty if none exists")
ELSE (EXISTS ${GMOCK_ALL_FILE})
  set (GMOCK_SRC "" CACHE FILEPATH "Location of gmock-all.cc or empty if none exists")  
ENDIF (EXISTS ${GMOCK_ALL_FILE})

# deal with the gmock library
find_library(GMOCK_LIB gmock)
IF (GMOCK_LIB STREQUAL GMOCK_LIB-NOTFOUND)
  set (GMOCK_LIB "")
ENDIF (GMOCK_LIB STREQUAL GMOCK_LIB-NOTFOUND)

# check that either GMOCK_LIB or GMOCK_SRC is set
IF (NOT GMOCK_SRC AND NOT GMOCK_LIB)
  Message(FATAL_ERROR "Gmock not found")
ENDIF (NOT GMOCK_SRC AND NOT GMOCK_LIB)

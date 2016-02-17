# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)

find_package(gmock REQUIRED)

add_executable(unitTests
  unit_tests.cpp
  ${GMOCK_SRC})

target_link_libraries(unitTests
  ctacatalogueunittests
  ctaexceptionunittests
  ctaiounittests
  ctalegacymsgunittests
  ctamessagesunittests
  ctaserverunittests
  ctatapereactorunittests
  ctatapeserverdaemonunittests
  ctatapeserverdriveunittests
  ctatapeserverfileunittests
  ctatapeserverscsiunittests
  ctatapeserverutilsunittests
  ctautilsunittests
  ctacommon
  ctacommonunittests
  ctaobjectstore
  ctaobjectstoreunittests
  ctanameserverunittests
  ctaremotensunittests
  ctaschedulerunittests
  ctaio
  ${GMOCK_LIB}
  gtest
  pthread)

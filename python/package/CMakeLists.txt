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

find_package (python REQUIRED)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/setup.py.in
  ${CMAKE_CURRENT_BINARY_DIR}/setup.py)

set (CTA_PYTHON_PACKAGE_SRC_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/cta/__init__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/cta/fst/__init__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/cta/fst/gc.py
  ${CMAKE_CURRENT_SOURCE_DIR}/cta/exceptions.py)

set (CTA_PYTHON_PACKAGE_RPMS
  ${CMAKE_CURRENT_BINARY_DIR}/dist/python-cta-${CTA_VERSION}.${CTA_RELEASE}.noarch.rpm
  ${CMAKE_CURRENT_BINARY_DIR}/dist/python-cta-${CTA_VERSION}.${CTA_RELEASE}.src.rpm)

add_custom_command(OUTPUT ${CTA_PYTHON_PACKAGE_RPMS}
  COMMAND ${PYTHON_PROGRAM} ${CMAKE_CURRENT_BINARY_DIR}/setup.py bdist_rpm --release ${CTA_RELEASE}.el7.cern
  DEPENDS ${CTA_PYTHON_PACKAGE_SRC_FILES})

add_custom_target(cta_python_package_rpm DEPENDS ${CTA_PYTHON_PACKAGE_RPMS})

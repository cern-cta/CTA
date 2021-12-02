# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2003-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 3.17)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

set(CTARAO_LIBRARY_SRCS
  RAOParams.cpp
  RAOManager.cpp
  RAOAlgorithm.cpp
  EnterpriseRAOAlgorithm.cpp
  RAOAlgorithmFactory.cpp
  EnterpriseRAOAlgorithmFactory.cpp
  LinearRAOAlgorithm.cpp
  RandomRAOAlgorithm.cpp
  NonConfigurableRAOAlgorithmFactory.cpp
  ConfigurableRAOAlgorithmFactory.cpp
  RAOAlgorithmFactoryFactory.cpp
  SLTFRAOAlgorithm.cpp
  RAOOptions.cpp
  CostHeuristic.cpp
  FilePositionInfos.cpp
  Position.cpp
  FilePositionEstimator.cpp
  InterpolationFilePositionEstimator.cpp
  RAOHelpers.cpp
  CTACostHeuristic.cpp
  RAOFile.cpp
  CostHeuristicFactory.cpp
  FilePositionEstimatorFactory.cpp
)

add_library (ctarao SHARED
  ${CTARAO_LIBRARY_SRCS})

install(TARGETS ctarao DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library(ctatapeserverraounittests SHARED
  RAOTest.cpp)

set_property(TARGET ctatapeserverraounittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverraounittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverraounittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

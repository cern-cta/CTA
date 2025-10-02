# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

set(CTARAO_LIBRARY_SRCS
  RAOParams.cpp
  RAOManager.cpp
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

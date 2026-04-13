# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

set(CTATAPEDRAO_LIBRARY_SRCS
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

add_library (ctatapedrao SHARED
  ${CTATAPEDRAO_LIBRARY_SRCS})

target_link_libraries(ctatapedrao ctacommon)

install(TARGETS ctatapedrao DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library(ctatapedraounittests SHARED
  RAOTest.cpp)

set_property(TARGET ctatapedraounittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedraounittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapedraounittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

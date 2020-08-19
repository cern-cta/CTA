cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

set(CTARAO_LIBRARY_SRCS
  RAOConfigurationData.cpp
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
  FilePosition.cpp
  Position.cpp
  FilePositionEstimator.cpp
  InterpolationFilePositionEstimator.cpp
)

add_library (ctarao SHARED
  ${CTARAO_LIBRARY_SRCS})

install(TARGETS ctarao DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
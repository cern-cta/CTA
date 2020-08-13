cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

set(CTARAO_LIBRARY_SRCS
  RAOConfig.cpp
  RAOManager.cpp
  RAOAlgorithm.cpp
  EnterpriseRAOAlgorithm.cpp
  RAOAlgorithmFactory.cpp
  EnterpriseRAOAlgorithmFactory.cpp
  LinearRAOAlgorithm.cpp
  RandomRAOAlgorithm.cpp
  NoParamRAOAlgorithmFactory.cpp
  RAOAlgorithmFactoryFactory.cpp
)

add_library (ctarao SHARED
  ${CTARAO_LIBRARY_SRCS})

install(TARGETS ctarao DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
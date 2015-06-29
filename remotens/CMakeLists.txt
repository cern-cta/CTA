cmake_minimum_required (VERSION 2.6)

set (CTA_REMOTENS_SRC_FILES
  EosNS.cpp
  MockRemoteNS.cpp
  RemoteNS.cpp
  RemoteNSDispatcher.cpp)

add_library (ctaremotens SHARED
  ${CTA_REMOTENS_SRC_FILES})

add_library (ctaremotenstest SHARED
  MockRemoteNS.cpp
  MockRemoteNSFactory.cpp 
  MockRemoteNSTest.cpp
  RemoteNSDispatcherTest.cpp 
  RemoteNSFactory.cpp)

target_link_libraries(ctaremotenstest
  ctaremotens)

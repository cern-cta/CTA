cmake_minimum_required (VERSION 2.6)

include_directories (${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR})

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  TapeMount.cpp
  LogicalLibrary.cpp
  MountType.cpp
  PositioningMethod.cpp
  RetrieveJob.cpp
  RetrieveMount.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SchedulerDatabaseFactory.cpp
  MountType.cpp
  OStoreDB/MemQueues.cpp
  OStoreDB/OStoreDB.cpp
  OStoreDB/OStoreDBWithAgent.cpp
  LabelMount.cpp)

find_package(Protobuf3 REQUIRED)
include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})
set_property(TARGET ctascheduler PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascheduler PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctascheduler DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctascheduler ctacommon ctaobjectstore ${PROTOBUF3_LIBRARIES} ctautils ctaeos)

#add_library (ctaschedulerutils SHARED
#  _old_prototype_DummyScheduler.cpp)

add_library (ctaschedulerunittests SHARED
  SchedulerDatabaseFactory.cpp
  SchedulerDatabaseTest.cpp
  SchedulerTest.cpp
  OStoreDB/OStoreDBTest.cpp)
set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctaschedulerunittests 
  ctascheduler)

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

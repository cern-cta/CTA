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

add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})
install (TARGETS ctascheduler DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctascheduler ctacommon ctaobjectstore protobuf ctautils ctadisk)

#add_library (ctaschedulerutils SHARED
#  _old_prototype_DummyScheduler.cpp)

add_library (ctaschedulerunittests SHARED
  SchedulerDatabaseFactory.cpp
  SchedulerDatabaseTest.cpp
  SchedulerTest.cpp)

target_link_libraries (ctaschedulerunittests 
  ctascheduler)

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

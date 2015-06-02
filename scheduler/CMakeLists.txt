cmake_minimum_required (VERSION 2.6)

set (CTA_SCHEDULER_SRC_FILES
  AdminHost.cpp
  AdminUser.cpp
  ArchivalFileTransfer.cpp
  ArchivalJob.cpp
  ArchivalJobQueues.cpp
  ArchivalJobState.cpp
  ArchivalMount.cpp
  ArchivalRequest.cpp
  ArchivalRoute.cpp
  ArchivalRouteId.cpp
  ArchiveToDirRequest.cpp
  ArchiveToFileRequest.cpp
  ConfigurationItem.cpp
  DirEntry.cpp
  DirIterator.cpp
  DiskException.cpp
  DriveQuota.cpp
  EosRemoteStorage.cpp
  FileAttribute.cpp
  FileSystemDirEntry.cpp
  FileSystemNode.cpp
  FileSystemStorageClass.cpp
  FileSystemStorageClasses.cpp
  FileTransfer.cpp
  LogicalLibrary.cpp
  MiddleTierAdmin.cpp
  MiddleTierUser.cpp
  Mount.cpp
  MountCriteria.cpp
  RemoteStorage.cpp
  RetrievalFileTransfer.cpp
  RetrievalJob.cpp
  RetrievalJobState.cpp
  RetrievalMount.cpp
  RetrievalRequest.cpp
  RetrieveToDirRequest.cpp
  RetrieveToFileRequest.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SecurityIdentity.cpp
  SqliteColumnNameToIndex.cpp
  StorageClass.cpp
  Tape.cpp
  TapeException.cpp
  TapeFileLocation.cpp
  TapePool.cpp
  UserIdentity.cpp
  UserGroup.cpp
  UserRequest.cpp
  VO.cpp)

add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})

add_library (ctaschedulertest SHARED
  MockSchedulerDatabase.cpp
  MockSchedulerDatabaseFactory.cpp
  SchedulerDatabaseFactory.cpp
  SchedulerTest.cpp)

target_link_libraries(ctaschedulertest
  ctascheduler)

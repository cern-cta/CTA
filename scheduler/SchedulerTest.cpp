/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/make_unique.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MountRequest.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/TapeMount.hpp"
#include "tests/TempFile.hpp"
#include "common/log/DummyLogger.hpp"
#include "objectstore/GarbageCollector.hpp"
#include "objectstore/BackendRadosTestSwitch.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/JobQueueType.hpp"
#include "objectstore/RepackIndex.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"
#include "common/Timer.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "objectstore/Algorithms.hpp"
#include "common/range.hpp"

#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

#include <exception>
#include <gtest/gtest.h>
#include <memory>
#include <utility>

namespace unitTests {

namespace {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct SchedulerTestParam {
  cta::SchedulerDatabaseFactory &dbFactory;

  SchedulerTestParam(
    cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {
 }
}; // struct SchedulerTestParam

}

/**
 * The scheduler test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class SchedulerTest: public ::testing::TestWithParam<SchedulerTestParam> {
public:

  SchedulerTest(): m_dummyLog("dummy", "dummy") {
  }

  class FailedToGetCatalogue: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler";
    }
  };

  class FailedToGetSchedulerDB: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get object store db.";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    // We do a deep reference to the member as the C++ compiler requires the function to be already defined if called implicitly
    const auto &factory = GetParam().dbFactory;
    // Get the OStore DB from the factory
    auto osdb = std::move(factory.create());
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface)
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory)
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.release()));

    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    //m_catalogue = cta::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = cta::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
    m_scheduler = cta::make_unique<Scheduler>(*m_catalogue, *m_db, 5, 2*1000*1000);
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
  }

  cta::catalogue::Catalogue &getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    if(nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::Scheduler &getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if(nullptr == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }

  cta::objectstore::OStoreDBWrapperInterface &getSchedulerDB() {
    cta::objectstore::OStoreDBWrapperInterface *const ptr = m_db.get();
    if(nullptr == ptr) {
      throw FailedToGetSchedulerDB();
    }
    return *ptr;
  }

  void setupDefaultCatalogue() {
    using namespace cta;
    auto & catalogue=getCatalogue();

    const std::string mountPolicyName = "mount_group";
    const uint64_t archivePriority = 1;
    const uint64_t minArchiveRequestAge = 2;
    const uint64_t retrievePriority = 3;
    const uint64_t minRetrieveRequestAge = 4;
    const uint64_t maxDrivesAllowed = 5;
    const std::string mountPolicyComment = "create mount group";

    ASSERT_TRUE(catalogue.getMountPolicies().empty());

    catalogue.createMountPolicy(
      s_adminOnAdminHost,
      mountPolicyName,
      archivePriority,
      minArchiveRequestAge,
      retrievePriority,
      minRetrieveRequestAge,
      maxDrivesAllowed,
      mountPolicyComment);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, group.maxDrivesAllowed);
    ASSERT_EQ(mountPolicyComment, group.comment);

    const std::string ruleComment = "create requester mount-rule";
    cta::common::dataStructures::UserIdentity userIdentity;
    catalogue.createRequesterMountRule(s_adminOnAdminHost, mountPolicyName, s_diskInstance, s_userName, ruleComment);

    const std::list<common::dataStructures::RequesterMountRule> rules = catalogue.getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(s_userName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(ruleComment, rule.comment);
    ASSERT_EQ(s_adminOnAdminHost.username, rule.creationLog.username);
    ASSERT_EQ(s_adminOnAdminHost.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

    common::dataStructures::StorageClass storageClass;
    storageClass.diskInstance = s_diskInstance;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.comment = "create storage class";
    m_catalogue->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const std::string vo = "vo";
    const bool tapePoolEncryption = false;
    catalogue.createTapePool(s_adminOnAdminHost, s_tapePoolName, vo, nbPartialTapes, tapePoolEncryption, tapePoolComment);
    const uint32_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.createArchiveRoute(s_adminOnAdminHost, s_diskInstance, s_storageClassName, copyNb, s_tapePoolName,
      archiveRouteComment);
  }

private:

  // Prevent copying
  SchedulerTest(const SchedulerTest &) = delete;

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &) = delete;

  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::objectstore::OStoreDBWrapperInterface> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
  
protected:

  // Default parameters for storage classes, etc...
  const std::string s_userName = "user_name";
  const std::string s_diskInstance = "disk_instance";
  const std::string s_storageClassName = "TestStorageClass";
  const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost = { "admin1", "host1" };
  const std::string s_tapePoolName = "TestTapePool";
  const std::string s_libraryName = "TestLogicalLibrary";
  const std::string s_vid = "TestVid";
  const std::string s_mediaType = "TestMediaType";
  const std::string s_vendor = "TestVendor";
  //TempFile m_tempSqliteFile;

}; // class SchedulerTest

TEST_P(SchedulerTest, archive_to_new_file) {
  using namespace cta;

  setupDefaultCatalogue();
  Scheduler &scheduler = getScheduler();
  
  cta::common::dataStructures::EntryLog creationLog;
  creationLog.host="host2";
  creationLog.time=0;
  creationLog.username="admin1";
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob="blob";
  diskFileInfo.group="group2";
  diskFileInfo.owner="cms_user";
  diskFileInfo.path="path/to/file";
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumType="ADLER32";
  request.checksumValue="1111";
  request.creationLog=creationLog;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::UserIdentity requester;
  requester.name = s_userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=s_storageClassName;

  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  const uint64_t archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass,
      request.requester, lc);
  scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  {
    auto rqsts = scheduler.getPendingArchiveJobs(lc);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const std::string pool = poolItor->first;
    ASSERT_TRUE(s_tapePoolName == pool);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->request.diskFileInfo.path);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(request.diskFileInfo.path) == remoteFiles.end());
  }
}

// smurray commented this test out on Mon 17 Jul 2017.  The test assumes that
// Scheduler::deleteArchive() calls SchedulerDatabase::deleteArchiveRequest().
// This fact is currently not true as Scheduler::deleteArchive() has been
// temporarily modified to only call Catalogue::deleteArchiveFile().
//
//TEST_P(SchedulerTest, delete_archive_request) {
//  using namespace cta;
//
//  Scheduler &scheduler = getScheduler();
//
//  setupDefaultCatalogue();
//
//  cta::common::dataStructures::EntryLog creationLog;
//  creationLog.host="host2";
//  creationLog.time=0;
//  creationLog.username="admin1";
//  cta::common::dataStructures::DiskFileInfo diskFileInfo;
//  diskFileInfo.recoveryBlob="blob";
//  diskFileInfo.group="group2";
//  diskFileInfo.owner="cms_user";
//  diskFileInfo.path="path/to/file";
//  cta::common::dataStructures::ArchiveRequest request;
//  request.checksumType="ADLER32";
//  request.checksumValue="1111";
//  request.creationLog=creationLog;
//  request.diskFileInfo=diskFileInfo;
//  request.diskFileID="diskFileID";
//  request.fileSize=100*1000*1000;
//  cta::common::dataStructures::UserIdentity requester;
//  requester.name = s_userName;
//  requester.group = "userGroup";
//  request.requester = requester;
//  request.srcURL="srcURL";
//  request.storageClass=s_storageClassName;
//
//  log::DummyLogger dl("");
//  log::LogContext lc(dl);
//  auto archiveFileId = scheduler.queueArchive(s_diskInstance, request, lc);
//
//  // Check that we have the file in the queues
//  // TODO: for this to work all the time, we need an index of all requests
//  // (otherwise we miss the selected ones).
//  // Could also be limited to querying by ID (global index needed)
//  bool found=false;
//  for (auto & tp: scheduler.getPendingArchiveJobs()) {
//    for (auto & req: tp.second) {
//      if (req.archiveFileID == archiveFileId)
//        found = true;
//    }
//  }
//  ASSERT_TRUE(found);
//
//  // Remove the request
//  cta::common::dataStructures::DeleteArchiveRequest dar;
//  dar.archiveFileID = archiveFileId;
//  dar.requester.group = "group1";
//  dar.requester.name = "user1";
//  scheduler.deleteArchive("disk_instance", dar);
//
//  // Validate that the request is gone.
//  found=false;
//  for (auto & tp: scheduler.getPendingArchiveJobs()) {
//    for (auto & req: tp.second) {
//      if (req.archiveFileID == archiveFileId)
//        found = true;
//    }
//  }
//  ASSERT_FALSE(found);
//}

TEST_P(SchedulerTest, archive_report_and_retrieve_new_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
  setupDefaultCatalogue();
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  uint64_t archiveFileId;
  {
    // Queue an archive request.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumType="ADLER32";
    request.checksumValue="1234abcd";
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  // Check that we have the file in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  bool found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs(lc)) {
    for (auto & req: tp.second) {
      if (req.archiveFileID == archiveFileId)
        found = true;
    }
  }
  ASSERT_TRUE(found);

  // Create the environment for the migration to happen (library + tape) 
  const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_mediaType, s_vendor, s_libraryName, s_tapePoolName, capacityInBytes,
    notDisabled, notFull, tapeComment);

  const bool lbpIsOn = true;
  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, "tape_drive", lbpIsOn);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TestTapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumType = "ADLER32";
    archiveJob->tapeFile.checksumValue = "1234abcd";
    archiveJob->tapeFile.compressedSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }
  
  {
    // Emulate the the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    eos::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = archiveFileId;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    scheduler.queueRetrieve("disk_instance", request, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  // Check that the retrieve request is queued
  {
    auto rqsts = scheduler.getPendingRetrieveJobs(lc);
    // We expect 1 tape with queued jobs
    ASSERT_EQ(1, rqsts.size());
    // We expect the queue to contain 1 job
    ASSERT_EQ(1, rqsts.cbegin()->second.size());
    // We expect the job to be single copy
    auto & job = rqsts.cbegin()->second.back();
    ASSERT_EQ(1, job.tapeCopies.size());
    // We expect the copy to be on the provided tape.
    ASSERT_TRUE(s_vid == job.tapeCopies.cbegin()->first);
    // Check the remote target
    ASSERT_EQ("dstURL", job.request.dstURL);
    // Check the archive file ID
    ASSERT_EQ(archiveFileId, job.request.archiveFileID);

    // Check that we can retrieve jobs by VID

    // Get the vid from the above job and submit a separate request for the same vid
    auto vid = rqsts.begin()->second.back().tapeCopies.begin()->first;
    auto rqsts_vid = scheduler.getPendingRetrieveJobs(vid, lc);
    // same tests as above
    ASSERT_EQ(1, rqsts_vid.size());
    auto &job_vid = rqsts_vid.back();
    ASSERT_EQ(1, job_vid.tapeCopies.size());
    ASSERT_TRUE(s_vid == job_vid.tapeCopies.cbegin()->first);
    ASSERT_EQ("dstURL", job_vid.request.dstURL);
    ASSERT_EQ(archiveFileId, job_vid.request.archiveFileID);
  }

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
    ASSERT_NE(nullptr, retrieveMount.get());
    std::unique_ptr<cta::RetrieveJob> retrieveJob;
    auto jobBatch = retrieveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(1, jobBatch.size());
    retrieveJob.reset(jobBatch.front().release());
    ASSERT_NE(nullptr, retrieveJob.get());
    retrieveJob->asyncSetSuccessful();
    std::queue<std::unique_ptr<cta::RetrieveJob> > jobQueue;
    jobQueue.push(std::move(retrieveJob));
    retrieveMount->flushAsyncSuccessReports(jobQueue, lc);
    jobBatch = retrieveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, jobBatch.size());
  }
}

TEST_P(SchedulerTest, archive_and_retrieve_failure) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
  setupDefaultCatalogue();
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  uint64_t archiveFileId;
  {
    // Queue an archive request.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumType="ADLER32";
    request.checksumValue="1234abcd";
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  // Check that we have the file in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  bool found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs(lc)) {
    for (auto & req: tp.second) {
      if (req.archiveFileID == archiveFileId)
        found = true;
    }
  }
  ASSERT_TRUE(found);

  // Create the environment for the migration to happen (library + tape) 
  const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_mediaType, s_vendor, s_libraryName, s_tapePoolName, capacityInBytes,
    notDisabled, notFull, tapeComment);

  const bool lbpIsOn = true;
  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, "tape_drive", lbpIsOn);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TestTapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumType = "ADLER32";
    archiveJob->tapeFile.checksumValue = "1234abcd";
    archiveJob->tapeFile.compressedSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }
  
  {
    // Emulate the the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    eos::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = archiveFileId;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.errorReportURL="null:";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    scheduler.queueRetrieve("disk_instance", request, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  
  // Try mounting the tape twice
  for(int mountPass = 0; mountPass < 2; ++mountPass)
  {
    // Check that the retrieve request is queued
    {
      auto rqsts = scheduler.getPendingRetrieveJobs(lc);
      // We expect 1 tape with queued jobs
      ASSERT_EQ(1, rqsts.size());
      // We expect the queue to contain 1 job
      ASSERT_EQ(1, rqsts.cbegin()->second.size());
      // We expect the job to be single copy
      auto & job = rqsts.cbegin()->second.back();
      ASSERT_EQ(1, job.tapeCopies.size());
      // We expect the copy to be on the provided tape.
      ASSERT_TRUE(s_vid == job.tapeCopies.cbegin()->first);
      // Check the remote target
      ASSERT_EQ("dstURL", job.request.dstURL);
      // Check the archive file ID
      ASSERT_EQ(archiveFileId, job.request.archiveFileID);

      // Check that we can retrieve jobs by VID

      // Get the vid from the above job and submit a separate request for the same vid
      auto vid = rqsts.begin()->second.back().tapeCopies.begin()->first;
      auto rqsts_vid = scheduler.getPendingRetrieveJobs(vid, lc);
      // same tests as above
      ASSERT_EQ(1, rqsts_vid.size());
      auto &job_vid = rqsts_vid.back();
      ASSERT_EQ(1, job_vid.tapeCopies.size());
      ASSERT_TRUE(s_vid == job_vid.tapeCopies.cbegin()->first);
      ASSERT_EQ("dstURL", job_vid.request.dstURL);
      ASSERT_EQ(archiveFileId, job_vid.request.archiveFileID);
    }

    {
      // Emulate a tape server by asking for a mount and then a file
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
      std::unique_ptr<cta::RetrieveMount> retrieveMount;
      retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
      ASSERT_NE(nullptr, retrieveMount.get());
      // The file should be retried three times
      for(int i = 0; i < 3; ++i)
      {
        std::list<std::unique_ptr<cta::RetrieveJob>> retrieveJobList = retrieveMount->getNextJobBatch(1,1,lc);
        if (!retrieveJobList.front().get()) {
          int __attribute__((__unused__)) debugI=i;
        }
        ASSERT_NE(0, retrieveJobList.size());
        // Validate we got the right file
        ASSERT_EQ(archiveFileId, retrieveJobList.front()->archiveFile.archiveFileID);
        retrieveJobList.front()->transferFailed("Retrieve failed (mount " + std::to_string(mountPass) +
                                                ", attempt " + std::to_string(i) + ")", lc);
      }
      // Then the request should be gone
      ASSERT_EQ(0, retrieveMount->getNextJobBatch(1,1,lc).size());
      // Set Agent timeout to zero for unit test
      {
        cta::objectstore::Agent rqAgent(getSchedulerDB().getAgentReference().getAgentAddress(), getSchedulerDB().getBackend());
        cta::objectstore::ScopedExclusiveLock ralk(rqAgent);
        rqAgent.fetch();
        rqAgent.setTimeout_us(0);
        rqAgent.commit();
      }
      // Garbage collect the request
      cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
      cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), getSchedulerDB().getBackend());
      gcAgent.initialize();
      gcAgent.insertAndRegisterSelf(lc);
      {
        cta::objectstore::GarbageCollector gc(getSchedulerDB().getBackend(), gcAgentRef, catalogue);
        gc.runOnePass(lc);
      }
      // Assign a new agent to replace the stale agent reference in the DB
      getSchedulerDB().replaceAgent(new objectstore::AgentReference("OStoreDBFactory2", dl));
    } // end of retries
  } // end of pass
  
  {
    // We expect the retrieve queue to be empty
    auto rqsts = scheduler.getPendingRetrieveJobs(lc);
    ASSERT_EQ(0, rqsts.size());
    // The failed queue should be empty
    auto retrieveJobFailedList = scheduler.getNextRetrieveJobsFailedBatch(10,lc);
    ASSERT_EQ(0, retrieveJobFailedList.size());
    // Emulate the the reporter process
    auto jobsToReport = scheduler.getNextRetrieveJobsToReportBatch(10, lc);
    ASSERT_EQ(1, jobsToReport.size());
    eos::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportRetrieveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextRetrieveJobsToReportBatch(10, lc).size());
  }

  {
    // There should be one failed job
    auto retrieveJobFailedList = scheduler.getNextRetrieveJobsFailedBatch(10,lc);
    ASSERT_EQ(1, retrieveJobFailedList.size());
  }
}

TEST_P(SchedulerTest, archive_and_retrieve_report_failure) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
  setupDefaultCatalogue();
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  uint64_t archiveFileId;
  {
    // Queue an archive request.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumType="ADLER32";
    request.checksumValue="1234abcd";
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  // Check that we have the file in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  bool found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs(lc)) {
    for (auto & req: tp.second) {
      if (req.archiveFileID == archiveFileId)
        found = true;
    }
  }
  ASSERT_TRUE(found);

  // Create the environment for the migration to happen (library + tape) 
  const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, "mediatype", "vendor", s_libraryName, s_tapePoolName,
    capacityInBytes, notDisabled, notFull, tapeComment);

  const bool lbpIsOn = true;
  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, "tape_drive", lbpIsOn);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TestTapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumType = "ADLER32";
    archiveJob->tapeFile.checksumValue = "1234abcd";
    archiveJob->tapeFile.compressedSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }
  
  {
    // Emulate the the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    eos::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = archiveFileId;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.errorReportURL="null:";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    scheduler.queueRetrieve("disk_instance", request, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  // Try mounting the tape twice
  for(int mountPass = 0; mountPass < 2; ++mountPass)
  {
    // Check that the retrieve request is queued
    {
      auto rqsts = scheduler.getPendingRetrieveJobs(lc);
      // We expect 1 tape with queued jobs
      ASSERT_EQ(1, rqsts.size());
      // We expect the queue to contain 1 job
      ASSERT_EQ(1, rqsts.cbegin()->second.size());
      // We expect the job to be single copy
      auto & job = rqsts.cbegin()->second.back();
      ASSERT_EQ(1, job.tapeCopies.size());
      // We expect the copy to be on the provided tape.
      ASSERT_TRUE(s_vid == job.tapeCopies.cbegin()->first);
      // Check the remote target
      ASSERT_EQ("dstURL", job.request.dstURL);
      // Check the archive file ID
      ASSERT_EQ(archiveFileId, job.request.archiveFileID);

      // Check that we can retrieve jobs by VID

      // Get the vid from the above job and submit a separate request for the same vid
      auto vid = rqsts.begin()->second.back().tapeCopies.begin()->first;
      auto rqsts_vid = scheduler.getPendingRetrieveJobs(vid, lc);
      // same tests as above
      ASSERT_EQ(1, rqsts_vid.size());
      auto &job_vid = rqsts_vid.back();
      ASSERT_EQ(1, job_vid.tapeCopies.size());
      ASSERT_TRUE(s_vid == job_vid.tapeCopies.cbegin()->first);
      ASSERT_EQ("dstURL", job_vid.request.dstURL);
      ASSERT_EQ(archiveFileId, job_vid.request.archiveFileID);
    }

    {
      // Emulate a tape server by asking for a mount and then a file
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
      std::unique_ptr<cta::RetrieveMount> retrieveMount;
      retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
      ASSERT_NE(nullptr, retrieveMount.get());
      // The file should be retried three times
      for(int i = 0; i < 3; ++i)
      {
        std::list<std::unique_ptr<cta::RetrieveJob>> retrieveJobList = retrieveMount->getNextJobBatch(1,1,lc);
        if (!retrieveJobList.front().get()) {
          int __attribute__((__unused__)) debugI=i;
        }
        ASSERT_NE(0, retrieveJobList.size());
        // Validate we got the right file
        ASSERT_EQ(archiveFileId, retrieveJobList.front()->archiveFile.archiveFileID);
        retrieveJobList.front()->transferFailed("Retrieve failed (mount " + std::to_string(mountPass) +
                                                ", attempt " + std::to_string(i) + ")", lc);
      }
      // Then the request should be gone
      ASSERT_EQ(0, retrieveMount->getNextJobBatch(1,1,lc).size());
      // Set Agent timeout to zero for unit test
      {
        cta::objectstore::Agent rqAgent(getSchedulerDB().getAgentReference().getAgentAddress(), getSchedulerDB().getBackend());
        cta::objectstore::ScopedExclusiveLock ralk(rqAgent);
        rqAgent.fetch();
        rqAgent.setTimeout_us(0);
        rqAgent.commit();
      }
      // Garbage collect the request
      cta::objectstore::AgentReference gcAgentRef("unitTestGarbageCollector", dl);
      cta::objectstore::Agent gcAgent(gcAgentRef.getAgentAddress(), getSchedulerDB().getBackend());
      gcAgent.initialize();
      gcAgent.insertAndRegisterSelf(lc);
      {
        cta::objectstore::GarbageCollector gc(getSchedulerDB().getBackend(), gcAgentRef, catalogue);
        gc.runOnePass(lc);
      }
      // Assign a new agent to replace the stale agent reference in the DB
      getSchedulerDB().replaceAgent(new objectstore::AgentReference("OStoreDBFactory2", dl));
    } // end of retries
  } // end of pass

  {
    // We expect the retrieve queue to be empty
    auto rqsts = scheduler.getPendingRetrieveJobs(lc);
    ASSERT_EQ(0, rqsts.size());
    // The failed queue should be empty
    auto retrieveJobFailedList = scheduler.getNextRetrieveJobsFailedBatch(10,lc);
    ASSERT_EQ(0, retrieveJobFailedList.size());
    // The failure should be on the jobs to report queue
    auto retrieveJobToReportList = scheduler.getNextRetrieveJobsToReportBatch(10,lc);
    ASSERT_EQ(1, retrieveJobToReportList.size());
    // Fail the report
    retrieveJobToReportList.front()->reportFailed("Report failed once", lc);
    // Job should still be on the report queue
    retrieveJobToReportList = scheduler.getNextRetrieveJobsToReportBatch(10,lc);
    ASSERT_EQ(1, retrieveJobToReportList.size());
    // Fail the report again
    retrieveJobToReportList.front()->reportFailed("Report failed twice", lc);
    // Job should be gone from the report queue
    retrieveJobToReportList = scheduler.getNextRetrieveJobsToReportBatch(10,lc);
    ASSERT_EQ(0, retrieveJobToReportList.size());
  }

  {
    // There should be one failed job
    auto retrieveJobFailedList = scheduler.getNextRetrieveJobsFailedBatch(10,lc);
    ASSERT_EQ(1, retrieveJobFailedList.size());
  }
}

TEST_P(SchedulerTest, retry_archive_until_max_reached) {
  using namespace cta;
  
  setupDefaultCatalogue();

  auto &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  uint64_t archiveFileId;
  {
    // Queue an archive request.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumType="ADLER32";
    request.checksumValue="1111";
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    request.archiveErrorReportURL="null:";
    archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  // Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_mediaType, s_vendor, s_libraryName, s_tapePoolName, capacityInBytes,
    notDisabled, notFull, tapeComment);

  const bool lbpIsOn = true;
  catalogue.tapeLabelled(s_vid, "tape_drive", lbpIsOn);

  {
    // Emulate a tape server by asking for a mount and then a file
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    // The file should be retried twice
    for (int i=0; i<=1; i++) {
      std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobList = archiveMount->getNextJobBatch(1,1,lc);
      if (!archiveJobList.front().get()) {
        int __attribute__((__unused__)) debugI=i;
      }
      ASSERT_NE(0, archiveJobList.size());
      // Validate we got the right file
      ASSERT_EQ(archiveFileId, archiveJobList.front()->archiveFile.archiveFileID);
      archiveJobList.front()->transferFailed("Archive failed", lc);
    }
    // Then the request should be gone
    ASSERT_EQ(0, archiveMount->getNextJobBatch(1,1,lc).size());
  }
}

TEST_P(SchedulerTest, retrieve_non_existing_file) {
  using namespace cta;
  
  setupDefaultCatalogue();
  
  Scheduler &scheduler = getScheduler();
  
  log::DummyLogger dl("", "");
  log::LogContext lc(dl);

  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = 12345;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    ASSERT_THROW(scheduler.queueRetrieve("disk_instance", request, lc), cta::exception::Exception);
  }
}

TEST_P(SchedulerTest, showqueues) {
  using namespace cta;
  
  setupDefaultCatalogue();
  
  Scheduler &scheduler = getScheduler();
  
  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  
  uint64_t archiveFileId __attribute__((unused));
  {
    // Queue an archive request.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.recoveryBlob="blob";
    diskFileInfo.group="group2";
    diskFileInfo.owner="cms_user";
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumType="ADLER32";
    request.checksumValue="1111";
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  // get the queues from scheduler
  auto queuesSummary = scheduler.getQueuesAndMountSummaries(lc);
  ASSERT_EQ(1, queuesSummary.size());
}

TEST_P(SchedulerTest, repack) {
  using namespace cta;
  
  setupDefaultCatalogue();
  
  Scheduler &scheduler = getScheduler();
  
  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  
  typedef cta::common::dataStructures::RepackInfo RepackInfo;
  typedef cta::common::dataStructures::RepackInfo::Status Status;
  
  // Create and then cancel repack
  common::dataStructures::SecurityIdentity cliId;
  std::string tape1 = "Tape";
  scheduler.queueRepack(cliId, tape1, "root://server/repackDir", common::dataStructures::RepackInfo::Type::MoveOnly, lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, repacks.size());
    auto repack = scheduler.getRepack(repacks.front().vid);
    ASSERT_EQ(tape1, repack.vid);
  }
  scheduler.cancelRepack(cliId, tape1, lc);
  ASSERT_EQ(0, scheduler.getRepacks().size());
  // Recreate a repack and get it moved to ToExpand
  scheduler.queueRepack(cliId, "Tape2", "root://server/repackDir", common::dataStructures::RepackInfo::Type::MoveOnly, lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, repacks.size());
    auto repack = scheduler.getRepack(repacks.front().vid);
    ASSERT_EQ("Tape2", repack.vid);
  }
  scheduler.promoteRepackRequestsToToExpand(lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, std::count_if(repacks.begin(), repacks.end(), [](RepackInfo &r){ return r.status == Status::ToExpand; }));
    ASSERT_EQ(1, repacks.size());
  }
}

TEST_P(SchedulerTest, getNextRepackRequestToExpand) {
  using namespace cta;
  
  setupDefaultCatalogue();
  
  Scheduler &scheduler = getScheduler();
  
  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  
  // Create a repack request
  common::dataStructures::SecurityIdentity cliId;
  std::string tape1 = "Tape";
  scheduler.queueRepack(cliId, tape1, "root://server/repackDir", common::dataStructures::RepackInfo::Type::MoveOnly, lc);
  
  std::string tape2 = "Tape2";
  scheduler.queueRepack(cliId,tape2,"root://server/repackDir",common::dataStructures::RepackInfo::Type::AddCopiesOnly,lc);
  
  //Test the repack request queued has status Pending
  ASSERT_EQ(scheduler.getRepack(tape1).status,common::dataStructures::RepackInfo::Status::Pending);
  ASSERT_EQ(scheduler.getRepack(tape2).status,common::dataStructures::RepackInfo::Status::Pending);
  
  //Change the repack request status to ToExpand
  scheduler.promoteRepackRequestsToToExpand(lc);
  
  //Test the getNextRepackRequestToExpand method that is supposed to retrieve the previously first inserted request
  auto repackRequestToExpand1 = scheduler.getNextRepackRequestToExpand();
  //Check vid
  ASSERT_EQ(repackRequestToExpand1.get()->getRepackInfo().vid,tape1);
  //Check status changed from Pending to ToExpand
  ASSERT_EQ(repackRequestToExpand1.get()->getRepackInfo().status,common::dataStructures::RepackInfo::Status::ToExpand);
  ASSERT_EQ(repackRequestToExpand1.get()->getRepackInfo().type,common::dataStructures::RepackInfo::Type::MoveOnly);
  
  //Test the getNextRepackRequestToExpand method that is supposed to retrieve the previously second inserted request
  auto repackRequestToExpand2 = scheduler.getNextRepackRequestToExpand();
  
  //Check vid
  ASSERT_EQ(repackRequestToExpand2.get()->getRepackInfo().vid,tape2);
  //Check status changed from Pending to ToExpand
  ASSERT_EQ(repackRequestToExpand2.get()->getRepackInfo().status,common::dataStructures::RepackInfo::Status::ToExpand);
  ASSERT_EQ(repackRequestToExpand2.get()->getRepackInfo().type,common::dataStructures::RepackInfo::Type::AddCopiesOnly);
  
  auto nullRepackRequest = scheduler.getNextRepackRequestToExpand();
  ASSERT_EQ(nullRepackRequest,nullptr);
}

TEST_P(SchedulerTest, expandRepackRequest) {
  using namespace cta;
  
  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();
  auto &schedulerDB = getSchedulerDB();
  
  setupDefaultCatalogue();
  
    
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  //Create an agent to represent this test process
  std::string agentReferenceName = "expandRepackRequestTest";
  std::unique_ptr<objectstore::AgentReference> agentReference(new objectstore::AgentReference(agentReferenceName, dl));
 
  
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const std::string comment = "Create tape";
  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";
  const std::string diskFileUser = "public_disk_user";
  const std::string diskFileGroup = "public_disk_group";
  const std::string diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  
  //Create a logical library in the catalogue
  catalogue.createLogicalLibrary(admin, s_libraryName, "Create logical library");
  
  uint64_t nbTapesToRepack = 10;
  uint64_t nbTapesForTest = 2; //corresponds to the targetAvailableRequests variable in the Scheduler::promoteRepackRequestsToToExpand() method
  
  std::vector<std::string> allVid;
  
  //Create the tapes from which we will retrieve
  for(uint64_t i = 1; i <= nbTapesToRepack ; ++i){
    std::ostringstream ossVid;
    ossVid << s_vid << "_" << i;
    std::string vid = ossVid.str();
    allVid.push_back(vid);
    catalogue.createTape(s_adminOnAdminHost,vid, s_mediaType, s_vendor, s_libraryName, s_tapePoolName, capacityInBytes,
      disabledValue, fullValue, comment);
  }
  
  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = s_diskInstance;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;
  const uint64_t compressedFileSize = archiveFileSize;

  //Simulate the writing of 10 files per tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    for(uint64_t i = 1; i<= nbTapesToRepack;++i){
      std::string currentVid = allVid.at(i-1);
      for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
        std::ostringstream diskFileId;
        diskFileId << (12345677 + archiveFileId);
        std::ostringstream diskFilePath;
        diskFilePath << "/public_dir/public_file_"<<i<<"_"<< j;
        auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
        auto & fileWritten = *fileWrittenUP;
        fileWritten.archiveFileId = archiveFileId++;
        fileWritten.diskInstance = storageClass.diskInstance;
        fileWritten.diskFileId = diskFileId.str();
        fileWritten.diskFilePath = diskFilePath.str();
        fileWritten.diskFileUser = diskFileUser;
        fileWritten.diskFileGroup = diskFileGroup;
        fileWritten.diskFileRecoveryBlob = diskFileRecoveryBlob;
        fileWritten.size = archiveFileSize;
        fileWritten.checksumType = checksumType;
        fileWritten.checksumValue = checksumValue;
        fileWritten.storageClassName = s_storageClassName;
        fileWritten.vid = currentVid;
        fileWritten.fSeq = j;
        fileWritten.blockId = j * 100;
        fileWritten.compressedSize = compressedFileSize;
        fileWritten.copyNb = 1;
        fileWritten.tapeDrive = tapeDrive;
        tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
      }
      //update the DB tape
      catalogue.filesWrittenToTape(tapeFilesWrittenCopy1);
      tapeFilesWrittenCopy1.clear();
    }
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    for(uint64_t i = 0; i < nbTapesToRepack ; ++i) {
      scheduler.queueRepack(admin,allVid.at(i),"root://repackData/buffer",common::dataStructures::RepackInfo::Type::MoveOnly,lc);
    }
    scheduler.waitSchedulerDbSubthreadsComplete();
    //scheduler.waitSchedulerDbSubthreadsComplete();
    for(uint64_t i = 0; i < nbTapesToRepack;++i){
      log::TimingList tl;
      utils::Timer t;
      if(i % nbTapesForTest == 0){
        //The promoteRepackRequestsToToExpand will only promote 2 RepackRequests to ToExpand status at a time.
        scheduler.promoteRepackRequestsToToExpand(lc);
        scheduler.waitSchedulerDbSubthreadsComplete();
      }
      auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
      //If we have expanded 2 repack requests, the getNextRepackRequestToExpand will return null as it is not possible
      //to promote more than 2 repack requests at a time. So we break here.
      if(repackRequestToExpand == nullptr) break;
      
      scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    }
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  //Here, we will only test that the two repack requests have been expanded
  {
    //The expandRepackRequest method should have queued nbArchiveFiles retrieve request corresponding to the previous files inserted in the catalogue
    uint64_t archiveFileId = 1;
    for(uint64_t i = 1 ; i <= nbTapesForTest ; ++i){
      //For each tapes
      std::string vid = allVid.at(i-1);
      std::list<common::dataStructures::RetrieveJob> retrieveJobs = scheduler.getPendingRetrieveJobs(vid,lc);
      ASSERT_EQ(retrieveJobs.size(),nbArchiveFilesPerTape);
      int j = 1;
      for(auto retrieveJob : retrieveJobs){
        //Test that the informations are correct for each file
        //ASSERT_EQ(retrieveJob.request.tapePool,s_tapePoolName);
        ASSERT_EQ(retrieveJob.request.archiveFileID,archiveFileId++);
        ASSERT_EQ(retrieveJob.fileSize,compressedFileSize);
        std::stringstream ss;
        ss<<"root://repackData/buffer/"<<allVid.at(i-1)<<"/"<<std::setw(9)<<std::setfill('0')<<j;
        ASSERT_EQ(retrieveJob.request.dstURL, ss.str());
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.copyNb,1);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.checksumType,checksumType);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.checksumValue,checksumValue);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.blockId,j*100);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.compressedSize,compressedFileSize);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.fSeq,j);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.vid,vid);
        ++j;
      }
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  //Now, we need to simulate a retrieve for each file
  {
    // Emulate a tape server by asking for nbTapesForTest mount and then all files
    for(uint64_t i = 1; i<= nbTapesForTest ;++i)
    {
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
      std::unique_ptr<cta::RetrieveMount> retrieveMount;
      retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
      ASSERT_NE(nullptr, retrieveMount.get());
      std::unique_ptr<cta::RetrieveJob> retrieveJob;

      std::list<std::unique_ptr<cta::RetrieveJob>> executedJobs;
      //For each tape we will see if the retrieve jobs are not null
      for(uint64_t j = 1; j<=nbArchiveFilesPerTape; ++j)
      {
        auto jobBatch = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);
        retrieveJob.reset(jobBatch.front().release());
        ASSERT_NE(nullptr, retrieveJob.get());
        executedJobs.push_back(std::move(retrieveJob));
      }
      //Now, report the retrieve jobs to be completed
        castor::tape::tapeserver::daemon::RecallReportPacker rrp(retrieveMount.get(),lc);

        rrp.startThreads();
        for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
        {
          rrp.reportCompletedJob(std::move(*it));
        }
        rrp.setDiskDone();
        rrp.setTapeDone();

        rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);

        rrp.reportEndOfSession();
        rrp.waitThread();

        ASSERT_EQ(rrp.allThreadsDone(),true);
      }
    
    uint64_t archiveFileId = 1;
    for(uint64_t i = 1; i<= nbTapesForTest ;++i)
    {
      //After the jobs reported as completed, we will test that all jobs have been put in 
      //the RetrieveQueueToReportToRepackForSuccess and that they have the status RJS_Succeeded
      {
        cta::objectstore::RootEntry re(schedulerDB.getBackend());
        cta::objectstore::ScopedExclusiveLock sel(re);
        re.fetch();

        //Get the retrieveQueueToReportToRepackForSuccess
        // The queue is named after the repack request: we need to query the repack index
        objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
        ri.fetchNoLock();
        std::string retrieveQueueToReportToRepackForSuccessAddress = re.getRetrieveQueueAddress(ri.getRepackRequestAddress(allVid.at(i-1)),cta::objectstore::JobQueueType::JobsToReportToRepackForSuccess);
        cta::objectstore::RetrieveQueue rq(retrieveQueueToReportToRepackForSuccessAddress,schedulerDB.getBackend());

        //Fetch the queue so that we can get the retrieveRequests from it
        cta::objectstore::ScopedExclusiveLock rql(rq);
        rq.fetch();

        //There should be nbArchiveFiles jobs in the retrieve queue
        ASSERT_EQ(rq.dumpJobs().size(),nbArchiveFilesPerTape);
        int j = 1;
        for (auto &job: rq.dumpJobs()) {
          //Create the retrieve request from the address of the job and the current backend
          cta::objectstore::RetrieveRequest retrieveRequest(job.address,schedulerDB.getBackend());
          retrieveRequest.fetchNoLock();
          uint32_t copyNb = job.copyNb;
          common::dataStructures::TapeFile tapeFile = retrieveRequest.getArchiveFile().tapeFiles[copyNb];
          common::dataStructures::RetrieveRequest schedulerRetrieveRequest = retrieveRequest.getSchedulerRequest();
          common::dataStructures::ArchiveFile archiveFile = retrieveRequest.getArchiveFile();

          //Testing tape file
          ASSERT_EQ(tapeFile.vid,allVid.at(i-1));
          ASSERT_EQ(tapeFile.blockId,j * 100);
          ASSERT_EQ(tapeFile.fSeq,j);
          ASSERT_EQ(tapeFile.checksumType, checksumType);
          ASSERT_EQ(tapeFile.checksumValue,checksumValue);
          ASSERT_EQ(tapeFile.compressedSize, compressedFileSize);

          //Testing scheduler retrieve request
          ASSERT_EQ(schedulerRetrieveRequest.archiveFileID,archiveFileId++);
          std::stringstream ss;
          ss<<"root://repackData/buffer/"<<allVid.at(i-1)<<"/"<<std::setw(9)<<std::setfill('0')<<j;
          ASSERT_EQ(schedulerRetrieveRequest.dstURL,ss.str());
          // TODO ASSERT_EQ(schedulerRetrieveRequest.isRepack,true);
          // TODO ASSERT_EQ(schedulerRetrieveRequest.tapePool,s_tapePoolName);
          std::ostringstream diskFilePath;
          diskFilePath << "/public_dir/public_file_"<<i<<"_"<<j;
          ASSERT_EQ(schedulerRetrieveRequest.diskFileInfo.path,diskFilePath.str());
          //Testing the retrieve request
          ASSERT_EQ(retrieveRequest.getRepackInfo().isRepack,true);
          // TODO ASSERT_EQ(retrieveRequest.getTapePool(),s_tapePoolName);
          ASSERT_EQ(retrieveRequest.getQueueType(),cta::objectstore::JobQueueType::JobsToReportToRepackForSuccess);
          ASSERT_EQ(retrieveRequest.getRetrieveFileQueueCriteria().mountPolicy,cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
          ASSERT_EQ(retrieveRequest.getActiveCopyNumber(),1);
          ASSERT_EQ(retrieveRequest.getJobStatus(job.copyNb),cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
          ASSERT_EQ(retrieveRequest.getJobs().size(),1);

          //Testing the archive file associated to the retrieve request
          ASSERT_EQ(archiveFile.storageClass,storageClass.name);
          ASSERT_EQ(archiveFile.diskInstance,storageClass.diskInstance);
          ++j;
        }
      }
    }
    // Now get the reports from the DB a check they are in the right queues
    {
      while (true) {
        auto rep = schedulerDB.getNextRepackReportBatch(lc);
        if (nullptr == rep) break;
        rep->report(lc);
      }
      // All the retrieve requests should be gone and replaced by archive requests.
      cta::objectstore::RootEntry re(schedulerDB.getBackend());
      re.fetchNoLock();
      typedef cta::objectstore::JobQueueType QueueType;
      for (auto queueType: {QueueType::FailedJobs, QueueType::JobsToReportToRepackForFailure, QueueType::JobsToReportToRepackForSuccess,
          QueueType::JobsToReportToUser, QueueType::JobsToTransferForRepack, QueueType::JobsToTransferForUser}) {
        ASSERT_EQ(0, re.dumpRetrieveQueues(queueType).size());
      }
      ASSERT_EQ(1, re.dumpArchiveQueues(QueueType::JobsToTransferForRepack).size());
      for (auto queueType: {QueueType::FailedJobs, QueueType::JobsToReportToRepackForFailure, QueueType::JobsToReportToRepackForSuccess,
          QueueType::JobsToReportToUser, QueueType::JobsToTransferForUser}) {
        ASSERT_EQ(0, re.dumpArchiveQueues(queueType).size());
      }
      // Now check we find all our requests in the archive queue.
      cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(s_tapePoolName, cta::objectstore::JobQueueType::JobsToTransferForRepack), 
          schedulerDB.getBackend());
      aq.fetchNoLock();
      std::set<uint64_t> archiveIdsSeen;
      for (auto aqj: aq.dumpJobs()) {
        cta::objectstore::ArchiveRequest ar(aqj.address, schedulerDB.getBackend());
        ar.fetchNoLock();
        common::dataStructures::ArchiveFile archiveFile = ar.getArchiveFile();
        // ArchiveFileId are 1-10 for tape 1 and 11-20 for tape 2.
        uint64_t tapeIndex = (archiveFile.archiveFileID - 1) / nbArchiveFilesPerTape + 1;
        uint64_t fileIndex = (archiveFile.archiveFileID - 1) % nbArchiveFilesPerTape + 1;
        ASSERT_LE(1, tapeIndex);
        ASSERT_GE(nbTapesForTest, tapeIndex);
        ASSERT_LE(1, fileIndex);
        ASSERT_GE(nbArchiveFilesPerTape, fileIndex);
        //Test the ArchiveRequest
        ASSERT_EQ(archiveFile.checksumType,checksumType);
        ASSERT_EQ(archiveFile.checksumValue,checksumValue);
        std::ostringstream diskFilePath;
        diskFilePath << "/public_dir/public_file_"<<tapeIndex<<"_"<<fileIndex;
        std::ostringstream diskFileId;
        diskFileId << (12345677 + archiveFile.archiveFileID);
        ASSERT_EQ(archiveFile.diskFileId,diskFileId.str());
        ASSERT_EQ(archiveFile.diskFileInfo.path,diskFilePath.str());
        ASSERT_EQ(archiveFile.diskFileInfo.group,diskFileGroup);
        ASSERT_EQ(archiveFile.diskFileInfo.owner,diskFileUser);
        ASSERT_EQ(archiveFile.diskFileInfo.recoveryBlob,diskFileRecoveryBlob);
        ASSERT_EQ(archiveFile.fileSize,archiveFileSize);
        ASSERT_EQ(archiveFile.storageClass,s_storageClassName);
        std::stringstream ss;
        ss<<"root://repackData/buffer/"<<allVid.at(tapeIndex-1)<<"/"<<std::setw(9)<<std::setfill('0')<<fileIndex;
        ASSERT_EQ(ar.getSrcURL(),ss.str());
        for(auto archiveJob : ar.dumpJobs()){
          ASSERT_EQ(archiveJob.status,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
          ASSERT_EQ(aq.getAddressIfSet(), archiveJob.owner);
        }
        archiveIdsSeen.insert(archiveFile.archiveFileID);
      }
      // Validate we got all the files we expected.
      ASSERT_EQ(20, archiveIdsSeen.size());
      for (auto ai: cta::range<uint64_t>(1,20)) {
        ASSERT_EQ(1, archiveIdsSeen.count(ai));
      }
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestFailedRetrieve) {
  using namespace cta;
  using namespace cta::objectstore;
  
  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();
  auto &schedulerDB = getSchedulerDB();
  cta::objectstore::Backend& backend = schedulerDB.getBackend();
  setupDefaultCatalogue();
  

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);
  
  //Create an agent to represent this test process
  cta::objectstore::AgentReference agentReference("expandRepackRequestTest", dl);
  cta::objectstore::Agent agent(agentReference.getAgentAddress(), backend);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);
  
  const uint64_t capacityInBytes = (uint64_t)10 * 1000 * 1000 * 1000 * 1000;
  const bool disabledValue = false;
  const bool fullValue = false;
  const std::string comment = "Create tape";
  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";
  const std::string diskFileUser = "public_disk_user";
  const std::string diskFileGroup = "public_disk_group";
  const std::string diskFileRecoveryBlob = "opaque_disk_file_recovery_contents";
  
  //Create a logical library in the catalogue
  catalogue.createLogicalLibrary(admin, s_libraryName, "Create logical library");
  
  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();
  catalogue.createTape(s_adminOnAdminHost,vid, s_mediaType, s_vendor, s_libraryName, s_tapePoolName, capacityInBytes,
    disabledValue, fullValue, comment);
  
  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.diskInstance = s_diskInstance;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string checksumType = "checksum_type";
  const std::string checksumValue = "checksum_value";
  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;
  const uint64_t compressedFileSize = archiveFileSize;
  
  //Simulate the writing of 10 files per tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = storageClass.diskInstance;
      fileWritten.diskFileId = diskFileId.str();
      fileWritten.diskFilePath = diskFilePath.str();
      fileWritten.diskFileUser = diskFileUser;
      fileWritten.diskFileGroup = diskFileGroup;
      fileWritten.diskFileRecoveryBlob = diskFileRecoveryBlob;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumType = checksumType;
      fileWritten.checksumValue = checksumValue;
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.compressedSize = compressedFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();
  
  {
    scheduler.queueRepack(admin,vid,"root://repackData/buffer",common::dataStructures::RepackInfo::Type::MoveOnly,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
    //scheduler.waitSchedulerDbSubthreadsComplete();
 
    log::TimingList tl;
    utils::Timer t;
    
    //The promoteRepackRequestsToToExpand will only promote 2 RepackRequests to ToExpand status at a time.
    scheduler.promoteRepackRequestsToToExpand(lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
    
    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    //If we have expanded 2 repack requests, the getNextRepackRequestToExpand will return null as it is not possible
    //to promote more than 2 repack requests at a time. So we break here.

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
    ASSERT_NE(nullptr, retrieveMount.get());
    std::unique_ptr<cta::RetrieveJob> retrieveJob;

    std::list<std::unique_ptr<cta::RetrieveJob>> executedJobs;
    //For each tape we will see if the retrieve jobs are not null
    for(uint64_t j = 1; j<=nbArchiveFilesPerTape; ++j)
    {
      auto jobBatch = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);
      retrieveJob.reset(jobBatch.front().release());
      ASSERT_NE(nullptr, retrieveJob.get());
      executedJobs.push_back(std::move(retrieveJob));
    }
    //Now, report the retrieve jobs to be completed
    castor::tape::tapeserver::daemon::RecallReportPacker rrp(retrieveMount.get(),lc);

    rrp.startThreads();
    
    //Report all jobs as succeeded except the first one
    auto it = executedJobs.begin();
    it++;
    while(it != executedJobs.end()){
      rrp.reportCompletedJob(std::move(*it));
      it++;
    }
    std::unique_ptr<cta::RetrieveJob> failedJobUniqPtr = std::move(*(executedJobs.begin()));
    rrp.reportFailedJob(std::move(failedJobUniqPtr),cta::exception::Exception("FailedJob expandRepackRequestFailedRetrieve"));
   
    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);

    rrp.reportEndOfSession();
    rrp.waitThread();

    ASSERT_EQ(rrp.allThreadsDone(),true);
    
    scheduler.waitSchedulerDbSubthreadsComplete();
    
    {
      for(int i = 0; i < 5; ++i){
        std::unique_ptr<cta::TapeMount> mount;
        mount.reset(scheduler.getNextMount(s_libraryName, "drive0", lc).release());
        ASSERT_NE(nullptr, mount.get());
        ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
        std::unique_ptr<cta::RetrieveMount> retrieveMount;
        retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
        ASSERT_NE(nullptr, retrieveMount.get());
        std::unique_ptr<cta::RetrieveJob> retrieveJob;

        std::list<std::unique_ptr<cta::RetrieveJob>> executedJobs;
        //For each tape we will see if the retrieve jobs are not null
        auto jobBatch = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);
        retrieveJob.reset(jobBatch.front().release());
        ASSERT_NE(nullptr, retrieveJob.get());
        
        castor::tape::tapeserver::daemon::RecallReportPacker rrp(retrieveMount.get(),lc);

        rrp.startThreads();
        
        rrp.reportFailedJob(std::move(retrieveJob),cta::exception::Exception("FailedJob for unit test expandRepackRequestFailedRetrieve"));
        
        rrp.setDiskDone();
        rrp.setTapeDone();

        rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting);

        rrp.reportEndOfSession();
        rrp.waitThread();
        ASSERT_EQ(rrp.allThreadsDone(),true);
      }
      
      {
        //Verify that the job is in the RetrieveQueueToReportToRepackForFailure
        cta::objectstore::RootEntry re(backend);
        cta::objectstore::ScopedExclusiveLock sel(re);
        re.fetch();

        //Get the retrieveQueueToReportToRepackForFailure
        // The queue is named after the repack request: we need to query the repack index
        objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
        ri.fetchNoLock();
        
        std::string retrieveQueueToReportToRepackForFailureAddress = re.getRetrieveQueueAddress(ri.getRepackRequestAddress(vid),cta::objectstore::JobQueueType::JobsToReportToRepackForFailure);
        cta::objectstore::RetrieveQueue rq(retrieveQueueToReportToRepackForFailureAddress,backend);

        //Fetch the queue so that we can get the retrieveRequests from it
        cta::objectstore::ScopedExclusiveLock rql(rq);
        rq.fetch();

        ASSERT_EQ(rq.dumpJobs().size(),1);
        for(auto& job: rq.dumpJobs()){
          ASSERT_EQ(1,job.copyNb);
          ASSERT_EQ(archiveFileSize,job.size);
        }
      }
      
      {
        Scheduler::RepackReportBatch reports = scheduler.getNextRepackReportBatch(lc);
        reports.report(lc);
        reports = scheduler.getNextRepackReportBatch(lc);
        reports.report(lc);
      }
      {
        //After the reporting, the RetrieveQueueToReportToRepackForFailure should not exist anymore.
        cta::objectstore::RootEntry re(backend);
        cta::objectstore::ScopedExclusiveLock sel(re);
        re.fetch();

        //Get the retrieveQueueToReportToRepackForFailure
        // The queue is named after the repack request: we need to query the repack index
        objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
        ri.fetchNoLock();
        
        ASSERT_THROW(re.getRetrieveQueueAddress(ri.getRepackRequestAddress(vid),cta::objectstore::JobQueueType::JobsToReportToRepackForFailure),cta::exception::Exception);
      }
    }
  }
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;
INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryRados)));
#endif
} // namespace unitTests

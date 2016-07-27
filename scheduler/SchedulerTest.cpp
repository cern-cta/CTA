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
#include "common/admin/AdminUser.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/make_unique.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/ArchiveRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MountRequest.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/TapeMount.hpp"
#include "tests/TempFile.hpp"

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

  SchedulerTest() {
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

  virtual void SetUp() {
    using namespace cta;

    const SchedulerTestParam &param = GetParam();
    m_db = param.dbFactory.create();
    const uint64_t nbConns = 1;
    //m_catalogue = cta::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = cta::make_unique<catalogue::InMemoryCatalogue>(nbConns);
    m_scheduler = cta::make_unique<Scheduler>(*m_catalogue, *m_db, 5, 2*1000*1000);
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
  }

  cta::catalogue::Catalogue &getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    if(NULL == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }
    
  cta::Scheduler &getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if(NULL == ptr) {
      throw FailedToGetScheduler();
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
    const bool tapePoolEncryption = false;
    catalogue.createTapePool(s_adminOnAdminHost, s_tapePoolName,
      nbPartialTapes, tapePoolEncryption, tapePoolComment);
    const uint16_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.createArchiveRoute(s_adminOnAdminHost, s_diskInstance, s_storageClassName, copyNb, s_tapePoolName,
      archiveRouteComment);
  }

private:

  // Prevent copying
  SchedulerTest(const SchedulerTest &) = delete;

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &) = delete;

  std::unique_ptr<cta::SchedulerDatabase> m_db;
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
  request.checksumType="Adler32";
  request.checksumValue="1111";
  request.creationLog=creationLog;
  request.diskpoolName="diskpool1";
  request.diskpoolThroughput=200*1000*1000;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::UserIdentity requester;
  requester.name = s_userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=s_storageClassName;

  scheduler.queueArchive(s_diskInstance, request);

  {
    auto rqsts = scheduler.getPendingArchiveJobs();
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

TEST_P(SchedulerTest, delete_archive_request) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  
  setupDefaultCatalogue();

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
  request.checksumType="Adler32";
  request.checksumValue="1111";
  request.creationLog=creationLog;
  request.diskpoolName="diskpool1";
  request.diskpoolThroughput=200*1000*1000;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::UserIdentity requester;
  requester.name = s_userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=s_storageClassName;

  auto archiveFileId = scheduler.queueArchive(s_diskInstance, request);
  
  // Check that we have the file in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  bool found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs()) {
    for (auto & req: tp.second) {
      if (req.archiveFileID == archiveFileId)
        found = true;
    }
  }
  ASSERT_TRUE(found);
  
  // Remove the request
  cta::common::dataStructures::DeleteArchiveRequest dar;
  dar.archiveFileID = archiveFileId;
  dar.requester.group = "group1";
  dar.requester.name = "user1";
  scheduler.deleteArchive("disk_instance", dar);
  
  // Validate that the request is gone.
  found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs()) {
    for (auto & req: tp.second) {
      if (req.archiveFileID == archiveFileId)
        found = true;
    }
  }
  ASSERT_FALSE(found);
}

TEST_P(SchedulerTest, archive_and_retrieve_new_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
  setupDefaultCatalogue();
  
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
    request.checksumType="adler32";
    request.checksumValue="1234abcd";
    request.creationLog=creationLog;
    request.diskpoolName="diskpool1";
    request.diskpoolThroughput=200*1000*1000;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.queueArchive(s_diskInstance, request);
  }
  
  // Check that we have the file in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  bool found=false;
  for (auto & tp: scheduler.getPendingArchiveJobs()) {
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
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed
    // the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0").release());
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::ARCHIVE, mount.get()->getMountType());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE((cta::ArchiveMount*)NULL, archiveMount.get());
    std::unique_ptr<cta::ArchiveJob> archiveJob;
    archiveJob.reset(archiveMount->getNextJob().release());
    ASSERT_NE((cta::ArchiveJob*)NULL, archiveJob.get());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumType = "adler32";
    archiveJob->tapeFile.checksumValue = "1234abcd";
    archiveJob->complete();
    archiveJob.reset(archiveMount->getNextJob().release());
    ASSERT_EQ((cta::ArchiveJob*)NULL, archiveJob.get());
    archiveMount->complete();
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
    request.entryLog = creationLog;
    request.diskpoolName = "diskpool1";
    request.diskpoolThroughput = 200*1000*1000;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    scheduler.queueRetrieve("disk_instance", request);
  }

  // Check that the retrieve request is queued
  {
    auto rqsts = scheduler.getPendingRetrieveJobs();
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
  }
  
  {
    // Emulate a tape server by asking for a mount and then a file (and succeed
    // the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0").release());
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::RETRIEVE, mount.get()->getMountType());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
    ASSERT_NE((cta::RetrieveMount*)NULL, retrieveMount.get());
    std::unique_ptr<cta::RetrieveJob> retrieveJob;
    retrieveJob.reset(retrieveMount->getNextJob().release());
    ASSERT_NE((cta::RetrieveJob*)NULL, retrieveJob.get());
    retrieveJob->complete();
    retrieveJob.reset(retrieveMount->getNextJob().release());
    ASSERT_EQ((cta::RetrieveJob*)NULL, retrieveJob.get());
  }
}

TEST_P(SchedulerTest, retry_archive_until_max_reached) {
  using namespace cta;
  
  setupDefaultCatalogue();

  auto &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  
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
    request.checksumType="Adler32";
    request.checksumValue="1111";
    request.creationLog=creationLog;
    request.diskpoolName="diskpool1";
    request.diskpoolThroughput=200*1000*1000;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::UserIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileId = scheduler.queueArchive(s_diskInstance, request);
  }
  
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
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, nullopt, capacityInBytes, notDisabled,
    notFull, tapeComment);
  
  {
    // Emulate a tape server by asking for a mount and then a file
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, "drive0").release());
    ASSERT_NE((cta::TapeMount*)NULL, mount.get());
    ASSERT_EQ(cta::MountType::ARCHIVE, mount.get()->getMountType());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE((cta::ArchiveMount*)NULL, archiveMount.get());
    // The file should be retried 10 times
    for (int i=0; i<=5; i++) {
      std::unique_ptr<cta::ArchiveJob> archiveJob(archiveMount->getNextJob());
      if (!archiveJob.get()) {
        int __attribute__((__unused__)) debugI=i;
      }
      ASSERT_NE((cta::ArchiveJob*)NULL, archiveJob.get());
      // Validate we got the right file
      ASSERT_EQ(archiveFileId, archiveJob->archiveFile.archiveFileID);
      archiveJob->failed(cta::exception::Exception("Archive failed"));
    }
    // Then the request should be gone
    std::unique_ptr<cta::ArchiveJob> archiveJob;
    archiveJob.reset(archiveMount->getNextJob().release());
    ASSERT_EQ((cta::ArchiveJob*)NULL, archiveJob.get());
  }
}

TEST_P(SchedulerTest, retrieve_non_existing_file) {
  using namespace cta;
  
  setupDefaultCatalogue();
  
  Scheduler &scheduler = getScheduler();

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
    request.entryLog = creationLog;
    request.diskpoolName = "diskpool1";
    request.diskpoolThroughput = 200*1000*1000;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    ASSERT_THROW(scheduler.queueRetrieve("disk_instance", request), cta::exception::Exception);
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

#undef TEST_RADOS
#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryRados)));
#endif
} // namespace unitTests


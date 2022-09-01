/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gtest/gtest.h>

#include <bits/unique_ptr.h>
#include <exception>
#include <memory>
#include <utility>

#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/Timer.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/TapeMount.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "tests/TempDirectory.hpp"
#include "tests/TempFile.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/PostgresSchedDB/PostgresSchedDBFactory.hpp"
#endif

#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#endif

namespace unitTests {

const uint32_t CMS_USER = 9751;
const uint32_t GROUP_2  = 9752;
const uint32_t PUBLIC_OWNER_UID = 9753;
const uint32_t PUBLIC_GID = 9754;

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
    const char *what() const noexcept {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const noexcept {
      return "Failed to get scheduler";
    }
  };

  class FailedToGetSchedulerDB: public std::exception {
  public:
    const char *what() const noexcept {
      return "Failed to get scheduler db.";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    // We do a deep reference to the member as the C++ compiler requires the function to be already defined if called implicitly
    const auto &factory = GetParam().dbFactory;
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    //m_catalogue = std::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = std::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
    // Get the SchedulerDatabase from the factory
    auto sdb = std::move(factory.create(m_catalogue));
    // We don't check the specific type of the SchedulerDatabase as we intend to ge generic
    m_db = std::move(sdb);
    m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_db, s_minFilesToWarrantAMount, s_minBytesToWarrantAMount);
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

  cta::SchedulerDatabase &getSchedulerDB() {
    cta::SchedulerDatabase *const ptr = m_db.get();
    if(nullptr == ptr) {
      throw FailedToGetSchedulerDB();
    }
    return *ptr;
  }

  void setupDefaultCatalogue() {
    using namespace cta;
    auto & catalogue=getCatalogue();

    const std::string mountPolicyName = s_mountPolicyName;
    const uint64_t archivePriority = s_archivePriority;
    const uint64_t minArchiveRequestAge = s_minArchiveRequestAge;
    const uint64_t retrievePriority = s_retrievePriority;
    const uint64_t minRetrieveRequestAge = s_minRetrieveRequestAge;
    const std::string mountPolicyComment = "create mount group";

    catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = mountPolicyName;
    mountPolicy.archivePriority = archivePriority;
    mountPolicy.minArchiveRequestAge = minArchiveRequestAge;
    mountPolicy.retrievePriority = retrievePriority;
    mountPolicy.minRetrieveRequestAge = minRetrieveRequestAge;
    mountPolicy.comment = mountPolicyComment;

    ASSERT_TRUE(catalogue.getMountPolicies().empty());

    catalogue.createMountPolicy(
      s_adminOnAdminHost,
      mountPolicy);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(mountPolicyComment, group.comment);

    m_catalogue->createDiskInstance(s_adminOnAdminHost, s_diskInstance, "comment");

    const std::string ruleComment = "create requester mount-rule";
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

    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = s_vo;
    vo.comment = "comment";
    vo.writeMaxDrives = 1;
    vo.readMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.diskInstanceName = s_diskInstance;
    m_catalogue->createVirtualOrganization(s_adminOnAdminHost,vo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create storage class";
    m_catalogue->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const bool tapePoolEncryption = false;
    const std::optional<std::string> tapePoolSupply("value for the supply pool mechanism");
    catalogue.createTapePool(s_adminOnAdminHost, s_tapePoolName, vo.name, nbPartialTapes, tapePoolEncryption, tapePoolSupply,
      tapePoolComment);
    const uint32_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.createArchiveRoute(s_adminOnAdminHost, s_storageClassName, copyNb, s_tapePoolName,
      archiveRouteComment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = s_mediaTypeCapacityInBytes;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    catalogue.createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "tape_drive";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.createTapeDrive(tapeDrive);
    const std::string driveName2 = "drive0";
    const auto tapeDrive2 = getDefaultTapeDrive(driveName2);
    catalogue.createTapeDrive(tapeDrive2);
  }

  cta::catalogue::CreateTapeAttributes getDefaultTape() {
    using namespace cta;

    catalogue::CreateTapeAttributes tape;
    tape.vid = s_vid;
    tape.mediaType = s_mediaType;
    tape.vendor = s_vendor;
    tape.logicalLibraryName = s_libraryName;
    tape.tapePoolName = s_tapePoolName;
    tape.full = false;
    tape.state = common::dataStructures::Tape::ACTIVE;
    tape.comment = "Comment";

    return tape;
  }

  cta::common::dataStructures::TapeDrive getDefaultTapeDrive(const std::string &driveName) {
    cta::common::dataStructures::TapeDrive tapeDrive;
    tapeDrive.driveName = driveName;
    tapeDrive.host = "admin_host";
    tapeDrive.logicalLibrary = "VLSTK10";
    tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
    tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
    tapeDrive.desiredUp = false;
    tapeDrive.desiredForceDown = false;
    tapeDrive.diskSystemName = "dummyDiskSystemName";
    tapeDrive.reservedBytes = 694498291384;
    tapeDrive.reservationSessionId = 0;
    return tapeDrive;
  }

private:

  // Prevent copying
  SchedulerTest(const SchedulerTest &) = delete;

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &) = delete;

  cta::log::DummyLogger m_dummyLog;
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;

protected:

  // Default parameters for storage classes, etc...
  const std::string s_userName = "user_name";
  const std::string s_diskInstance = "disk_instance";
  const std::string s_storageClassName = "TestStorageClass";
  const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost = { "admin1", "host1" };
  const std::string s_tapePoolName = "TapePool";
  const std::string s_libraryName = "TestLogicalLibrary";
  const std::string s_vid = "TestVid";
  const std::string s_mediaType = "TestMediaType";
  const std::string s_vendor = "TestVendor";
  const std::string s_mountPolicyName = "mount_group";
  const std::string s_repackMountPolicyName = "repack_mount_group";
  const bool s_defaultRepackDisabledTapeFlag = false;
  const bool s_defaultRepackNoRecall = false;
  const uint64_t s_minFilesToWarrantAMount = 5;
  const uint64_t s_minBytesToWarrantAMount = 2*1000*1000;
  const uint64_t s_archivePriority = 1;
  const uint64_t s_minArchiveRequestAge = 2;
  const uint64_t s_retrievePriority = 3;
  const uint64_t s_minRetrieveRequestAge = 4;
  const uint64_t s_mediaTypeCapacityInBytes = 10;
  const std::string s_vo = "vo";
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
  diskFileInfo.gid=GROUP_2;
  diskFileInfo.owner_uid=CMS_USER;
  diskFileInfo.path="path/to/file";
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumBlob.insert(cta::checksum::ADLER32, "1111");
  request.creationLog=creationLog;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::RequesterIdentity requester;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  const bool libraryIsDisabled = true;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.tapeLabelled(s_vid, driveName);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    //Test that no mount is available when a logical library is disabled
    ASSERT_EQ(nullptr, mount.get());
    catalogue.setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Emulate the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    disk::DiskReporterFactory factory;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
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
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

TEST_P(SchedulerTest, archive_report_and_retrieve_new_file_with_specific_mount_policy) {
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  const bool libraryIsDisabled = true;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, "tape_drive");

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    //Test that no mount is available when a logical library is disabled
    ASSERT_EQ(nullptr, mount.get());
    catalogue.setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Emulate the the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    disk::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  {
    //create custom mount policy for retrieve
    catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = "custom_mount_policy";
    mountPolicy.archivePriority = s_archivePriority;
    mountPolicy.minArchiveRequestAge = s_minArchiveRequestAge;
    mountPolicy.retrievePriority = s_retrievePriority;
    mountPolicy.minRetrieveRequestAge = s_minRetrieveRequestAge;
    mountPolicy.comment = "custom mount policy";

    catalogue.createMountPolicy(s_adminOnAdminHost, mountPolicy);
  }

  {
    //queue retrieve
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = archiveFileId;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    request.mountPolicy = "custom_mount_policy";
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
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

TEST_P(SchedulerTest, archive_report_and_retrieve_new_dual_copy_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();

  // Setup catalogue for dual tape copies
  const std::string tapePool1Name = "tape_pool_1";
  const std::string tapePool2Name = "tape_pool_2";
  const std::string dualCopyStorageClassName = "dual_copy";
  {
    using namespace cta;

    const std::string mountPolicyName = s_mountPolicyName;
    const uint64_t archivePriority = s_archivePriority;
    const uint64_t minArchiveRequestAge = s_minArchiveRequestAge;
    const uint64_t retrievePriority = s_retrievePriority;
    const uint64_t minRetrieveRequestAge = s_minRetrieveRequestAge;
    const std::string mountPolicyComment = "create mount group";

    catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = mountPolicyName;
    mountPolicy.archivePriority = archivePriority;
    mountPolicy.minArchiveRequestAge = minArchiveRequestAge;
    mountPolicy.retrievePriority = retrievePriority;
    mountPolicy.minRetrieveRequestAge = minRetrieveRequestAge;
    mountPolicy.comment = mountPolicyComment;

    ASSERT_TRUE(catalogue.getMountPolicies().empty());

    catalogue.createMountPolicy(
      s_adminOnAdminHost,
      mountPolicy);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(mountPolicyComment, group.comment);

    cta::common::dataStructures::DiskInstance di;
    di.name = s_diskInstance;
    di.comment = "comment";
    catalogue.createDiskInstance(s_adminOnAdminHost, di.name, di.comment);

    const std::string ruleComment = "create requester mount-rule";
    catalogue.createRequesterMountRule(s_adminOnAdminHost, mountPolicyName, di.name, s_userName, ruleComment);

    const std::list<common::dataStructures::RequesterMountRule> rules = catalogue.getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(s_userName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(ruleComment, rule.comment);
    ASSERT_EQ(s_adminOnAdminHost.username, rule.creationLog.username);
    ASSERT_EQ(s_adminOnAdminHost.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = s_vo;
    vo.comment = "comment";
    vo.writeMaxDrives = 1;
    vo.readMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.diskInstanceName = s_diskInstance;
    catalogue.createVirtualOrganization(s_adminOnAdminHost,vo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = dualCopyStorageClassName;
    storageClass.nbCopies = 2;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create dual copy storage class";
    catalogue.createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePool1Comment = "Tape-pool for copy number 1";
    const std::string tapePool2Comment = "Tape-pool for copy number 2";
    const bool tapePoolEncryption = false;
    const std::optional<std::string> tapePoolSupply("value for the supply pool mechanism");
    catalogue.createTapePool(s_adminOnAdminHost, tapePool1Name, vo.name, nbPartialTapes, tapePoolEncryption,
      tapePoolSupply, tapePool1Comment);
    catalogue.createTapePool(s_adminOnAdminHost, tapePool2Name, vo.name, nbPartialTapes, tapePoolEncryption,
      tapePoolSupply, tapePool2Comment);

    const std::string archiveRoute1Comment = "Archive-route for copy number 1";
    const std::string archiveRoute2Comment = "Archive-route for copy number 2";
    const uint32_t archiveRoute1CopyNb = 1;
    const uint32_t archiveRoute2CopyNb = 2;
    catalogue.createArchiveRoute(s_adminOnAdminHost, dualCopyStorageClassName, archiveRoute1CopyNb, tapePool1Name,
      archiveRoute1Comment);
    catalogue.createArchiveRoute(s_adminOnAdminHost, dualCopyStorageClassName, archiveRoute2CopyNb, tapePool2Name,
      archiveRoute1Comment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = s_mediaTypeCapacityInBytes;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    catalogue.createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "tape_drive";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.createTapeDrive(tapeDrive);
    const std::string driveName2 = "drive0";
    const auto tapeDrive2 = getDefaultTapeDrive(driveName2);
    catalogue.createTapeDrive(tapeDrive2);
  }

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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=dualCopyStorageClassName;
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

  // Create the environment for the migration of copy 1 to happen (library +
  // tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = true;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  const std::string copy1TapeVid = "copy_1_tape";
  {
    using namespace cta;

    catalogue::CreateTapeAttributes tape;
    tape.vid = copy1TapeVid;
    tape.mediaType = s_mediaType;
    tape.vendor = s_vendor;
    tape.logicalLibraryName = s_libraryName;
    tape.tapePoolName = tapePool1Name;
    tape.full = false;
    tape.state = common::dataStructures::Tape::ACTIVE;
    tape.comment = "Comment";
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(copy1TapeVid, driveName);

  // Archive copy 1 to tape
  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);

    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    //Test that no mount is available when a logical library is disabled
    ASSERT_EQ(nullptr, mount.get());
    catalogue.setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ(tapePool1Name, mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ(copy1TapeVid, mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Check that there are no jobs to report because only 1 copy of a dual copy
    // file has been archived
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_EQ(0, jobsToReport.size());
  }

  // Create the environment for the migration of copy 2 to happen (library +
  // tape)
  catalogue.setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,true);
  const std::string copy2TapeVid = "copy_2_tape";
  {
    using namespace cta;

    catalogue::CreateTapeAttributes tape;
    tape.vid = copy2TapeVid;
    tape.mediaType = s_mediaType;
    tape.vendor = s_vendor;
    tape.logicalLibraryName = s_libraryName;
    tape.tapePoolName = tapePool2Name;
    tape.full = false;
    tape.state = common::dataStructures::Tape::ACTIVE;
    tape.comment = "Comment";
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  catalogue.tapeLabelled(copy2TapeVid, driveName);

  // Archive copy 2 to tape
  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    //Test that no mount is available when a logical library is disabled
    ASSERT_EQ(nullptr, mount.get());
    catalogue.setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ(tapePool2Name, mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ(copy2TapeVid, mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 2;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Emulate the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    disk::DiskReporterFactory factory;
    log::TimingList timings;
    utils::Timer t;
    scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  // Check that there are now two tape copies in the catalogue
  {
    common::dataStructures::RequesterIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    std::optional<std::string> activity;
    const common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      catalogue.prepareToRetrieveFile(s_diskInstance, archiveFileId, requester, activity, lc);
    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    std::map<uint8_t, common::dataStructures::TapeFile> copyNbToTape;
    for (auto &tapeFile: queueCriteria.archiveFile.tapeFiles) {
      if(copyNbToTape.end() != copyNbToTape.find(tapeFile.copyNb)) {
        FAIL() << "Duplicate copyNb: vid=" << tapeFile.vid << " copyNb=" << (uint32_t)(tapeFile.copyNb);
      }
      copyNbToTape[tapeFile.copyNb] = tapeFile;
    }

    {
      const auto tapeItor = copyNbToTape.find(1);
      ASSERT_NE(copyNbToTape.end(), tapeItor);

      const auto tapeFile = tapeItor->second;
      ASSERT_EQ(copy1TapeVid, tapeFile.vid);
      ASSERT_EQ(1, tapeFile.copyNb);
    }

    {
      const auto tapeItor = copyNbToTape.find(2);
      ASSERT_NE(copyNbToTape.end(), tapeItor);

      const auto tapeFile = tapeItor->second;
      ASSERT_EQ(copy2TapeVid, tapeFile.vid);
      ASSERT_EQ(2, tapeFile.copyNb);
    }
  }

  // Queue the retrieve request
  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
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
    ASSERT_EQ("dstURL", job_vid.request.dstURL);
    ASSERT_EQ(archiveFileId, job_vid.request.archiveFileID);
  }

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, driveName);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Emulate the the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    disk::DiskReporterFactory factory;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
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
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
    } // end of retries
  } // end of pass

  {
    // We expect the retrieve queue to be empty
    auto rqsts = scheduler.getPendingRetrieveJobs(lc);
    ASSERT_EQ(0, rqsts.size());
    // The failed queue should be empty
    auto retrieveJobFailedList = scheduler.getNextRetrieveJobsFailedBatch(10,lc);
    ASSERT_EQ(0, retrieveJobFailedList.size());
    // Emulate the reporter process
    auto jobsToReport = scheduler.getNextRetrieveJobsToReportBatch(10, lc);
    ASSERT_EQ(1, jobsToReport.size());
    disk::DiskReporterFactory factory;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, driveName);

  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TestVid", mi->existingOrNextMounts.front().vid);
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> failedToReportArchiveJobs;
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  {
    // Emulate the reporter process reporting successful transfer to tape to the disk system
    auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
    ASSERT_NE(0, jobsToReport.size());
    disk::DiskReporterFactory factory;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
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
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, "1111");
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.tapeLabelled(s_vid, driveName);

  {
    // Emulate a tape server by asking for a mount and then a file
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
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
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, "1111");
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
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
  unitTests::TempDirectory tempDirectory;
  setupDefaultCatalogue();

  Scheduler &scheduler = getScheduler();
  cta::catalogue::Catalogue& catalogue = getCatalogue();

  log::DummyLogger dl("", "");
  log::LogContext lc(dl);

  typedef cta::common::dataStructures::RepackInfo RepackInfo;
  typedef cta::common::dataStructures::RepackInfo::Status Status;

   // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);

  common::dataStructures::SecurityIdentity cliId;
  cliId.host = "host";
  cliId.username = s_userName;
  std::string tape1 = "Tape";

  {
    auto tape = getDefaultTape();
    tape.vid = tape1;
    catalogue.createTape(cliId, tape);
  }

  //The queueing of a repack request should fail if the tape to repack is not full
  cta::SchedulerDatabase::QueueRepackRequest qrr(tape1,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackDisabledTapeFlag,s_defaultRepackNoRecall);
  ASSERT_THROW(scheduler.queueRepack(cliId, qrr, lc),cta::exception::UserError);
  //The queueing of a repack request in a vid that does not exist should throw an exception
  qrr.m_vid = "NOT_EXIST";
  ASSERT_THROW(scheduler.queueRepack(cliId, qrr, lc),cta::exception::UserError);

  catalogue.setTapeFull(cliId,tape1,true);

  // Create and then cancel repack
  qrr.m_vid = tape1;
  scheduler.queueRepack(cliId, qrr, lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, repacks.size());
    auto repack = scheduler.getRepack(repacks.front().vid);
    ASSERT_EQ(tape1, repack.vid);
  }
  scheduler.cancelRepack(cliId, tape1, lc);
  ASSERT_EQ(0, scheduler.getRepacks().size());
  // Recreate a repack and get it moved to ToExpand
  std::string tape2 = "Tape2";
  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
    tape.vid = tape2;
    tape.full = true;
    catalogue.createTape(cliId, tape);
  }
  qrr.m_vid = tape2;
  scheduler.queueRepack(cliId, qrr, lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, repacks.size());
    auto repack = scheduler.getRepack(repacks.front().vid);
    ASSERT_EQ(tape2, repack.vid);
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
  unitTests::TempDirectory tempDirectory;

  setupDefaultCatalogue();

  Scheduler &scheduler = getScheduler();
  catalogue::Catalogue& catalogue = getCatalogue();

  log::DummyLogger dl("", "");
  log::LogContext lc(dl);

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);

  common::dataStructures::SecurityIdentity cliId;
  cliId.host = "host";
  cliId.username = s_userName;
  std::string tape1 = "Tape";
  {
    auto tape = getDefaultTape();
    tape.vid = tape1;
    tape.full = true;
    catalogue.createTape(cliId, tape);
  }

  //Queue the first repack request
  cta::SchedulerDatabase::QueueRepackRequest qrr(tape1,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackDisabledTapeFlag,s_defaultRepackNoRecall);
  scheduler.queueRepack(cliId, qrr, lc);

  std::string tape2 = "Tape2";

  {
    auto tape = getDefaultTape();
    tape.vid = tape2;
    tape.full = true;
    catalogue.createTape(cliId, tape);
  }

  //Queue the second repack request
  qrr.m_vid = tape2;
  qrr.m_repackType = common::dataStructures::RepackInfo::Type::AddCopiesOnly;
  scheduler.queueRepack(cliId,qrr,lc);

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
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestRetrieveFailed) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestArchiveSuccess) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestArchiveFailed) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestDisabledTape) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestBrokenTape) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, noMountIsTriggeredWhenTapeIsDisabled) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, DISABLED_archiveReportMultipleAndQueueRetrievesWithActivities) {
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

  // We want to virtually archive files on 10 different tapes that will be asked for by different activities.
  // Activity A will have a weight of .4, B 0.3, and this allows partially predicting the mount order for them:
  // (A or B) (the other) A B A B A (A or B) (the other) A.
  // We hence need to create files on 10 different tapes and recall them with the respective activities.
  std::map<size_t, uint64_t> archiveFileIds;
  // Generates a list of 10 numbers from 0 to 9
  const uint8_t NUMBER_OF_FILES = 10;
  for (auto i = 0; i < NUMBER_OF_FILES; i++) {
    // Queue several archive requests.
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    diskFileInfo.path += std::to_string(i);
    cta::common::dataStructures::ArchiveRequest request;
    request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    request.creationLog=creationLog;
    request.diskFileInfo=diskFileInfo;
    request.diskFileID="diskFileID";
    request.diskFileID += std::to_string(i);
    request.fileSize=100*1000*1000;
    cta::common::dataStructures::RequesterIdentity requester;
    requester.name = s_userName;
    requester.group = "userGroup";
    request.requester = requester;
    request.srcURL="srcURL";
    request.storageClass=s_storageClassName;
    archiveFileIds[i] = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
    scheduler.queueArchiveWithGivenId(archiveFileIds[i], s_diskInstance, request, lc);
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // Check that we have the files in the queues
  // TODO: for this to work all the time, we need an index of all requests
  // (otherwise we miss the selected ones).
  // Could also be limited to querying by ID (global index needed)
  std::map<size_t, bool> found;
  for (auto & tp: scheduler.getPendingArchiveJobs(lc)) {
    for (auto & req: tp.second) {
      for (auto i = 0; i < NUMBER_OF_FILES; i++)
        if (req.archiveFileID == archiveFileIds.at(i))
          found[i] = true;
    }
  }
  for (auto i = 0; i < NUMBER_OF_FILES; i++) {
    ASSERT_NO_THROW(found.at(i));
    ASSERT_TRUE(found.at(i));
  }

  // Create the environment for the migrations to happen (library + tapes)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const std::string driveName = "tape_drive";
  for (auto i = 0; i < NUMBER_OF_FILES; i++) {
    auto tape = getDefaultTape();
    std::string vid = s_vid + std::to_string(i);
    tape.vid = vid;
    catalogue.createTape(s_adminOnAdminHost, tape);
    catalogue.tapeLabelled(vid, driveName);
  }


  {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    for (auto i = 0; i < NUMBER_OF_FILES; i++) {
      (void) i; // ignore unused variable
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
      auto & osdb=getSchedulerDB();
      auto mi=osdb.getMountInfo(lc);
      ASSERT_EQ(1, mi->existingOrNextMounts.size());
      ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());
      std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
      ASSERT_NE(nullptr, archiveJobBatch.front().get());
      ASSERT_EQ(1, archiveJobBatch.size());
      std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
      archiveJob->tapeFile.blockId = 1;
      archiveJob->tapeFile.fSeq = 1;
      archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
      archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
      archiveJob->tapeFile.copyNb = 1;
      archiveJob->validate();
      std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
      std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
      std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
      sDBarchiveJobBatch.emplace(std::move(archiveJob));
      archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems, failedToReportArchiveJobs, lc);
      // Mark the tape full so we get one file per tape.
      archiveMount->setTapeFull();
      archiveMount->complete();
    }
  }

  {
    // Emulate the reporter process reporting successful transfer to tape to the disk system
    // The jobs get reported by tape, so we need to report 10*1 file (one per tape).
    for (auto i = 0; i < NUMBER_OF_FILES; i++) {
      auto jobsToReport = scheduler.getNextArchiveJobsToReportBatch(10, lc);
      ASSERT_EQ(1, jobsToReport.size());
      disk::DiskReporterFactory factory;
      log::TimingList timings;
      utils::Timer t;
      scheduler.reportArchiveJobsBatch(jobsToReport, factory, timings, t, lc);
    }
    ASSERT_EQ(0, scheduler.getNextArchiveJobsToReportBatch(10, lc).size());
  }

  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    for (auto i = 0; i < NUMBER_OF_FILES; i++) {
      cta::common::dataStructures::RetrieveRequest request;
      request.archiveFileID = archiveFileIds.at(i);
      request.creationLog = creationLog;
      request.diskFileInfo = diskFileInfo;
      request.dstURL = "dstURL";
      request.requester.name = s_userName;
      request.requester.group = "userGroup";
      if (i < 6)
        request.activity = "A";
      else
        request.activity = "B";
      scheduler.queueRetrieve(s_diskInstance, request, lc);
    }
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  // Check that the retrieve requests are queued
  {
    auto rqsts = scheduler.getPendingRetrieveJobs(lc);
    // We expect 10 tape with queued jobs
    ASSERT_EQ(10, rqsts.size());
    // We expect each queue to contain 1 job
    for (auto & q: rqsts) {
      ASSERT_EQ(1, q.second.size());
      // We expect the job to be single copy
      auto & job = q.second.back();
      ASSERT_EQ(1, job.tapeCopies.size());
      // Check the remote target
      ASSERT_EQ("dstURL", job.request.dstURL);
    }
    // We expect each tape to be seen
    for (auto i = 0; i < NUMBER_OF_FILES; i++) {
      ASSERT_NO_THROW(rqsts.at(s_vid + std::to_string(i)));
    }
  }


  enum ExpectedActivity {
    Unknown,
    A,
    B
  };

  std::vector<ExpectedActivity> expectedActivities = { Unknown, Unknown, A, B, A, B, A, Unknown, Unknown, A};
  size_t i=0;
  for (auto ea: expectedActivities) {
    // Emulate a tape server by asking for a mount and then a file (and succeed the transfer)
    std::unique_ptr<cta::TapeMount> mount;
    std::string drive="drive";
    drive += std::to_string(++i);
    mount.reset(scheduler.getNextMount(s_libraryName, drive, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
    ASSERT_TRUE((bool)mount.get()->getActivity());
    if (ea != Unknown) {
      std::string expectedActivity(ea==A?"A":"B"), activity(mount.get()->getActivity().value());
      ASSERT_EQ(expectedActivity, activity);
    }
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

TEST_P(SchedulerTest, expandRepackRequestAddCopiesOnly) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestShouldFailIfArchiveRouteMissing) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestMoveAndAddCopies){
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, cancelRepackRequest) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, getNextMountEmptyArchiveForRepackIfNbFilesQueuedIsLessThan2TimesMinFilesWarrantAMount) {
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, getNextMountBrokenOrDisabledTapeShouldNotReturnAMount) {
  //Queue 2 archive requests in two different logical libraries
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

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);

  auto tape = getDefaultTape();
  {
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, driveName);

  {
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
  }

  uint64_t archiveFileId;

  // Queue an archive request.
  cta::common::dataStructures::EntryLog creationLog;
  creationLog.host="host2";
  creationLog.time=0;
  creationLog.username="admin1";
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.gid=GROUP_2;
  diskFileInfo.owner_uid=CMS_USER;
  diskFileInfo.path="path/to/file";
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
  request.creationLog=creationLog;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::RequesterIdentity requester;
  requester.name = s_userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=s_storageClassName;
  archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass, request.requester, lc);
  scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);

  scheduler.waitSchedulerDbSubthreadsComplete();

  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::BROKEN,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::BROKEN, std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::DISABLED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::list<std::unique_ptr<cta::ArchiveJob>> archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_NE(nullptr, archiveJobBatch.front().get());
    std::unique_ptr<ArchiveJob> archiveJob = std::move(archiveJobBatch.front());
    archiveJob->tapeFile.blockId = 1;
    archiveJob->tapeFile.fSeq = 1;
    archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, 0x1234abcd);
    archiveJob->tapeFile.fileSize = archiveJob->archiveFile.fileSize;
    archiveJob->tapeFile.copyNb = 1;
    archiveJob->validate();
    std::queue<std::unique_ptr <cta::ArchiveJob >> sDBarchiveJobBatch;
    std::queue<cta::catalogue::TapeItemWritten> sTapeItems;
    std::queue<std::unique_ptr <cta::SchedulerDatabase::ArchiveJob >> failedToReportArchiveJobs;
    sDBarchiveJobBatch.emplace(std::move(archiveJob));
    archiveMount->reportJobsBatchTransferred(sDBarchiveJobBatch, sTapeItems,failedToReportArchiveJobs, lc);
    archiveJobBatch = archiveMount->getNextJobBatch(1,1,lc);
    ASSERT_EQ(0, archiveJobBatch.size());
    archiveMount->complete();
  }

  //Queue a retrieve request for the archived file
  {
    cta::common::dataStructures::EntryLog creationLog;
    creationLog.host="host2";
    creationLog.time=0;
    creationLog.username="admin1";
    cta::common::dataStructures::DiskFileInfo diskFileInfo;
    diskFileInfo.gid=GROUP_2;
    diskFileInfo.owner_uid=CMS_USER;
    diskFileInfo.path="path/to/file";
    cta::common::dataStructures::RetrieveRequest request;
    request.archiveFileID = archiveFileId;
    request.creationLog = creationLog;
    request.diskFileInfo = diskFileInfo;
    request.dstURL = "dstURL";
    request.requester.name = s_userName;
    request.requester.group = "userGroup";
    scheduler.queueRetrieve(s_diskInstance, request, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::BROKEN,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::BROKEN,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::DISABLED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
}

TEST_P(SchedulerTest, repackRetrieveRequestsFailToFetchDiskSystem){
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, expandRepackRequestShouldThrowIfUseBufferNotRecallButNoDirectoryCreated){
  using namespace cta;
  unitTests::TempDirectory tempDirectory;

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();

  setupDefaultCatalogue();
  catalogue.createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "eos:ctaeos:default", 10, "no comment");
  catalogue.createDiskSystem({"user", "host"}, "repackBuffer", "diskInstance", "diskInstanceSpace", tempDirectory.path(), 10L*1000*1000*1000, 15*60, "no comment");

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, "Create logical library");

  {
    auto tape = getDefaultTape();
    tape.full = true;
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files per tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  checksum::ChecksumBlob checksumBlob;
  checksumBlob.insert(cta::checksum::ADLER32, "1234");
  {
    uint64_t archiveFileId = 1;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob = checksumBlob;
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = s_vid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  bool noRecall = true;

  cta::SchedulerDatabase::QueueRepackRequest qrr(s_vid,"file://DOES_NOT_EXIST",common::dataStructures::RepackInfo::Type::MoveOnly,
  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackDisabledTapeFlag,noRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  scheduler.promoteRepackRequestsToToExpand(lc);
  scheduler.waitSchedulerDbSubthreadsComplete();
  auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
  log::TimingList tl;
  utils::Timer t;
  ASSERT_THROW(scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc),cta::ExpandRepackRequestException);
}

TEST_P(SchedulerTest, expandRepackRequestShouldNotThrowIfTapeDisabledButNoRecallFlagProvided){
  using namespace cta;
  unitTests::TempDirectory tempDirectory;

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();

  setupDefaultCatalogue();
  catalogue.createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "eos:ctaeos:default", 10, "no comment");
  catalogue.createDiskSystem({"user", "host"}, "repackBuffer", "diskInstance", "diskInstanceSpace",tempDirectory.path(), 10L*1000*1000*1000, 15*60, "no comment");

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, "Create logical library");

  {
    auto tape = getDefaultTape();
    tape.full = true;
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files per tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  checksum::ChecksumBlob checksumBlob;
  checksumBlob.insert(cta::checksum::ADLER32, "1234");
  {
    uint64_t archiveFileId = 1;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob = checksumBlob;
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = s_vid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  bool noRecall = true;
  std::string pathRepackBuffer = "file://"+tempDirectory.path();
  tempDirectory.append("/"+s_vid);
  tempDirectory.mkdir();
  cta::SchedulerDatabase::QueueRepackRequest qrr(s_vid,pathRepackBuffer,common::dataStructures::RepackInfo::Type::MoveOnly,
  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackDisabledTapeFlag,noRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  scheduler.promoteRepackRequestsToToExpand(lc);
  scheduler.waitSchedulerDbSubthreadsComplete();
  auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
  log::TimingList tl;
  utils::Timer t;
  ASSERT_NO_THROW(scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc));
}

TEST_P(SchedulerTest, archiveMaxDrivesVoInFlightChangeScheduleMount){
  using namespace cta;

  setupDefaultCatalogue();
  Scheduler &scheduler = getScheduler();
  auto & catalogue = getCatalogue();
  cta::common::dataStructures::EntryLog creationLog;
  creationLog.host="host2";
  creationLog.time=0;
  creationLog.username="admin1";
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.gid=GROUP_2;
  diskFileInfo.owner_uid=CMS_USER;
  diskFileInfo.path="path/to/file";
  cta::common::dataStructures::ArchiveRequest request;
  request.checksumBlob.insert(cta::checksum::ADLER32, "1111");
  request.creationLog=creationLog;
  request.diskFileInfo=diskFileInfo;
  request.diskFileID="diskFileID";
  request.fileSize=100*1000*1000;
  cta::common::dataStructures::RequesterIdentity requester;
  requester.name = s_userName;
  requester.group = "userGroup";
  request.requester = requester;
  request.srcURL="srcURL";
  request.storageClass=s_storageClassName;

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  auto tape = getDefaultTape();
  catalogue.createTape(s_adminOnAdminHost, tape);

  const std::string driveName = "tape_drive";

  catalogue.tapeLabelled(s_vid, driveName);


  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  const uint64_t archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass,
      request.requester, lc);
  scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  catalogue.modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,0);

  {
    // Emulate a tape server
    std::unique_ptr<cta::TapeMount> mount;
    // This first initialization is normally done by the dataSession function.
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, lc);
    scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Up, lc);
    bool nextMount = scheduler.getNextMountDryRun(s_libraryName, driveName, lc);
    //nextMount should be false as the VO write max drives is 0
    ASSERT_FALSE(nextMount);
    catalogue.modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,1);
    //Reset the VO write max drives to a positive number should give a new mount
    nextMount = scheduler.getNextMountDryRun(s_libraryName,driveName,lc);
    ASSERT_TRUE(nextMount);
  }
}

TEST_P(SchedulerTest, retrieveMaxDrivesVoInFlightChangeScheduleMount)
{
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, retrieveArchiveAllTypesMaxDrivesVoInFlightChangeScheduleMount)
{
  ASSERT_EQ(0,1);
}

TEST_P(SchedulerTest, getQueuesAndMountSummariesTest)
{
  ASSERT_EQ(0,1);
}

//This test tests what is described in the use case ticket
// high priority Archive job not scheduled when Repack is running : https://gitlab.cern.ch/cta/operations/-/issues/150
TEST_P(SchedulerTest, getNextMountWithArchiveForUserAndArchiveForRepackShouldReturnBothMountsArchiveMinRequestAge){
  ASSERT_EQ(0,1);
}

#ifdef CTA_PGSCHED
static cta::PostgresSchedDBFactory PostgresSchedDBFactoryStatic;

INSTANTIATE_TEST_CASE_P(PostgresSchedulerDBPlusMockGenericSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(PostgresSchedDBFactoryStatic)));
#else
#error Generic SchedulerTest not configured for current scheduler type
#endif
} // namespace unitTests

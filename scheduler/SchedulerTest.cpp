/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
#include <unistd.h>
#include <utility>

#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/Timer.hpp"
#include "objectstore/Algorithms.hpp"
#include "objectstore/BackendRadosTestSwitch.hpp"
#include "objectstore/GarbageCollector.hpp"
#include "objectstore/RepackIndex.hpp"
#include "objectstore/RootEntry.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
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
 * This structure is used to describe a tape state change during the 'triggerTapeStateChangeValidScenarios' test
 */
struct TriggerTapeStateChangeBehaviour {
  cta::common::dataStructures::Tape::State fromState;
  cta::common::dataStructures::Tape::State toState;
  cta::common::dataStructures::Tape::State observedState;
  bool changeRaisedException;
  bool cleanupFlagActivated;
};

/**
 * This structure is used to parameterize scheduler tests.
 */
struct SchedulerTestParam {
  cta::SchedulerDatabaseFactory &m_dbFactory;
  std::optional<TriggerTapeStateChangeBehaviour> m_triggerTapeStateChangeBehaviour;

  SchedulerTestParam(
    cta::SchedulerDatabaseFactory &dbFactory):
    m_dbFactory(dbFactory) {
  }

  SchedulerTestParam(
          cta::SchedulerDatabaseFactory &dbFactory,
          TriggerTapeStateChangeBehaviour triggerTapeStateChangeBehaviour):
          m_dbFactory(dbFactory),
          m_triggerTapeStateChangeBehaviour(triggerTapeStateChangeBehaviour) {
  }
}; // struct SchedulerTestParam

std::ostream& operator<<(std::ostream& os, const SchedulerTestParam& c) {
  if (!c.m_triggerTapeStateChangeBehaviour.has_value()) {
    return os << "Test";
  } else {
    auto & params = c.m_triggerTapeStateChangeBehaviour.value();
    return os << "{ "
              << "\"from\": "               << "\"" << cta::common::dataStructures::Tape::stateToString(params.fromState)     << "\"" << ", "
              << "\"to\": "                 << "\"" << cta::common::dataStructures::Tape::stateToString(params.toState)       << "\"" << ", "
              << "\"expected_state\": "     << "\"" << cta::common::dataStructures::Tape::stateToString(params.observedState) << "\"" << ", "
              << "\"expected_exception\": " << "\"" << (params.changeRaisedException ? "yes" : "no")                          << "\"" << ", "
              << "\"expected_cleanup\": "   << "\"" << (params.cleanupFlagActivated  ? "yes" : "no")                          << "\"" << " }";
  }
}

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
      return "Failed to get object store db.";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    // We do a deep reference to the member as the C++ compiler requires the function to be already defined if called implicitly
    const auto &factory = GetParam().m_dbFactory;
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    //m_catalogue = std::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = std::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
    // Get the OStore DB from the factory
    auto osdb = std::move(factory.create(m_catalogue));
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface)
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory)
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface*>(osdb.release()));
    m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_db, s_minFilesToWarrantAMount, s_minBytesToWarrantAMount);
    objectstore::Helpers::flushRetrieveQueueStatisticsCache();
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

    ASSERT_TRUE(catalogue.MountPolicy()->getMountPolicies().empty());

    catalogue.MountPolicy()->createMountPolicy(
      s_adminOnAdminHost,
      mountPolicy);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(mountPolicyComment, group.comment);

    m_catalogue->DiskInstance()->createDiskInstance(s_adminOnAdminHost, s_diskInstance, "comment");

    const std::string ruleComment = "create requester mount-rule";
    catalogue.RequesterMountRule()->createRequesterMountRule(s_adminOnAdminHost, mountPolicyName, s_diskInstance, s_userName, ruleComment);

    const auto rules = catalogue.RequesterMountRule()->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(s_userName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(ruleComment, rule.comment);
    ASSERT_EQ(s_adminOnAdminHost.username, rule.creationLog.username);
    ASSERT_EQ(s_adminOnAdminHost.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

    // VO for user requests
    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = s_vo;
    vo.comment = "comment";
    vo.writeMaxDrives = 1;
    vo.readMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.diskInstanceName = s_diskInstance;
    vo.isRepackVo = false;
    m_catalogue->VO()->createVirtualOrganization(s_adminOnAdminHost,vo);

    // VO for repacking
    cta::common::dataStructures::VirtualOrganization repackVo;
    repackVo.name = s_repack_vo;
    repackVo.comment = "comment";
    repackVo.writeMaxDrives = 1;
    repackVo.readMaxDrives = 1;
    repackVo.maxFileSize = 0;
    repackVo.diskInstanceName = s_diskInstance;
    repackVo.isRepackVo = true;
    m_catalogue->VO()->createVirtualOrganization(s_adminOnAdminHost,repackVo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create storage class";
    m_catalogue->StorageClass()->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const bool tapePoolEncryption = false;
    const std::optional<std::string> tapePoolSupply("value for the supply pool mechanism");
    catalogue.TapePool()->createTapePool(s_adminOnAdminHost, s_tapePoolName, vo.name, nbPartialTapes, tapePoolEncryption,
      tapePoolSupply, tapePoolComment);
    const uint32_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.ArchiveRoute()->createArchiveRoute(s_adminOnAdminHost, s_storageClassName, copyNb, s_tapePoolName,
      archiveRouteComment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = s_mediaTypeCapacityInBytes;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    catalogue.MediaType()->createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "tape_drive";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
    const std::string driveName2 = "drive0";
    const auto tapeDrive2 = getDefaultTapeDrive(driveName2);
    catalogue.DriveState()->createTapeDrive(tapeDrive2);
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
    cta::common::dataStructures::EntryLog log = {"admin", "myHost", time(nullptr)};
    tapeDrive.creationLog = log;
    tapeDrive.lastModificationLog = log;
    return tapeDrive;
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
  const std::string s_tapePoolName = "TapePool";
  const std::string s_libraryName = "TestLogicalLibrary";
  const std::string s_vid = "TESTVID";
  const std::string s_mediaType = "TestMediaType";
  const std::string s_vendor = "TestVendor";
  const std::string s_mountPolicyName = "mount_group";
  const std::string s_repackMountPolicyName = "repack_mount_group";
  const bool s_defaultRepackNoRecall = false;
  const uint64_t s_minFilesToWarrantAMount = 5;
  const uint64_t s_minBytesToWarrantAMount = 2*1000*1000;
  const uint64_t s_archivePriority = 1;
  const uint64_t s_minArchiveRequestAge = 2;
  const uint64_t s_retrievePriority = 3;
  const uint64_t s_minRetrieveRequestAge = 4;
  const uint64_t s_mediaTypeCapacityInBytes = 10;
  const std::string s_vo = "vo";
  const std::string s_repack_vo = "repack_vo";
  //TempFile m_tempSqliteFile;

}; // class SchedulerTest

/**
 * The trigger tape state change is a parameterized test.  In addition to the default parameters,
 * it should take a 'TriggerTapeStateChangeBehaviour' reference.
 */
class SchedulerTestTriggerTapeStateChangeBehaviour : public SchedulerTest {};

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

  int ctrl_var = 0;
  while (ctrl_var == 0) usleep(10000);
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
//  diskFileInfo.gid=GROUP_2;
//  diskFileInfo.owner_uid=CMS_USER;
//  diskFileInfo.path="path/to/file";
//  cta::common::dataStructures::ArchiveRequest request;
//  request.checksumBlob.insert(cta::checksum::ADLER32, "1111");
//  request.creationLog=creationLog;
//  request.diskFileInfo=diskFileInfo;
//  request.diskFileID="diskFileID";
//  request.fileSize=100*1000*1000;
//  cta::common::dataStructures::RequesterIdentity requester;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.Tape()->tapeLabelled(s_vid, driveName);

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
    catalogue.LogicalLibrary()->setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TESTVID", mi->existingOrNextMounts.front().vid);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(s_vid, "tape_drive");

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
    catalogue.LogicalLibrary()->setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
    //continue our test
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser, mount.get()->getMountType());
    mount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);
    auto & osdb=getSchedulerDB();
    auto mi=osdb.getMountInfo(lc);
    ASSERT_EQ(1, mi->existingOrNextMounts.size());
    ASSERT_EQ("TapePool", mi->existingOrNextMounts.front().tapePool);
    ASSERT_EQ("TESTVID", mi->existingOrNextMounts.front().vid);
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

    catalogue.MountPolicy()->createMountPolicy(s_adminOnAdminHost, mountPolicy);
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

    ASSERT_TRUE(catalogue.MountPolicy()->getMountPolicies().empty());

    catalogue.MountPolicy()->createMountPolicy(
      s_adminOnAdminHost,
      mountPolicy);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.MountPolicy()->getMountPolicies();
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
    catalogue.DiskInstance()->createDiskInstance(s_adminOnAdminHost, di.name, di.comment);

    const std::string ruleComment = "create requester mount-rule";
    catalogue.RequesterMountRule()->createRequesterMountRule(s_adminOnAdminHost, mountPolicyName, di.name, s_userName,
      ruleComment);

    const auto rules = catalogue.RequesterMountRule()->getRequesterMountRules();
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
    vo.isRepackVo = false;
    catalogue.VO()->createVirtualOrganization(s_adminOnAdminHost,vo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = dualCopyStorageClassName;
    storageClass.nbCopies = 2;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create dual copy storage class";
    catalogue.StorageClass()->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePool1Comment = "Tape-pool for copy number 1";
    const std::string tapePool2Comment = "Tape-pool for copy number 2";
    const bool tapePoolEncryption = false;
    const std::optional<std::string> tapePoolSupply("value for the supply pool mechanism");
    catalogue.TapePool()->createTapePool(s_adminOnAdminHost, tapePool1Name, vo.name, nbPartialTapes, tapePoolEncryption,
      tapePoolSupply, tapePool1Comment);
    catalogue.TapePool()->createTapePool(s_adminOnAdminHost, tapePool2Name, vo.name, nbPartialTapes, tapePoolEncryption,
      tapePoolSupply, tapePool2Comment);

    const std::string archiveRoute1Comment = "Archive-route for copy number 1";
    const std::string archiveRoute2Comment = "Archive-route for copy number 2";
    const uint32_t archiveRoute1CopyNb = 1;
    const uint32_t archiveRoute2CopyNb = 2;
    catalogue.ArchiveRoute()->createArchiveRoute(s_adminOnAdminHost, dualCopyStorageClassName, archiveRoute1CopyNb, tapePool1Name,
      archiveRoute1Comment);
    catalogue.ArchiveRoute()->createArchiveRoute(s_adminOnAdminHost, dualCopyStorageClassName, archiveRoute2CopyNb, tapePool2Name,
      archiveRoute1Comment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = s_mediaTypeCapacityInBytes;
    mediaType.cartridge = "cartridge";
    mediaType.comment = "comment";
    catalogue.MediaType()->createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "tape_drive";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
    const std::string driveName2 = "drive0";
    const auto tapeDrive2 = getDefaultTapeDrive(driveName2);
    catalogue.DriveState()->createTapeDrive(tapeDrive2);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  const std::string copy1TapeVid = "COPY_1_TAPE";
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
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(copy1TapeVid, driveName);

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
    catalogue.LogicalLibrary()->setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
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
  catalogue.LogicalLibrary()->setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,true);
  const std::string copy2TapeVid = "COPY_2_TAPE";
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
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  catalogue.Tape()->tapeLabelled(copy2TapeVid, driveName);

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
    catalogue.LogicalLibrary()->setLogicalLibraryDisabled(s_adminOnAdminHost,s_libraryName,false);
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
      catalogue.TapeFile()->prepareToRetrieveFile(s_diskInstance, archiveFileId, requester, activity, lc);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(s_vid, driveName);

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
    ASSERT_EQ("TESTVID", mi->existingOrNextMounts.front().vid);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(s_vid, driveName);

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
    ASSERT_EQ("TESTVID", mi->existingOrNextMounts.front().vid);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.Tape()->tapeLabelled(s_vid, driveName);

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  common::dataStructures::SecurityIdentity cliId;
  cliId.host = "host";
  cliId.username = s_userName;
  std::string tape1 = "TAPE";

  {
    auto tape = getDefaultTape();
    tape.vid = tape1;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(cliId, tape);
  }

  //The queueing of a repack request should fail if the tape to repack is not full
  cta::SchedulerDatabase::QueueRepackRequest qrr(tape1,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
  ASSERT_THROW(scheduler.queueRepack(cliId, qrr, lc),cta::exception::UserError);
  //The queueing of a repack request in a vid that does not exist should throw an exception
  qrr.m_vid = "NOT_EXIST";
  ASSERT_THROW(scheduler.queueRepack(cliId, qrr, lc),cta::exception::UserError);

  catalogue.Tape()->setTapeFull(cliId,tape1,true);

  int ctrl_var = 0;
  while( ctrl_var == 0) usleep(10000);

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
  std::string tape2 = "TAPE2";
  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
    tape.vid = tape2;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    tape.full = true;
    catalogue.Tape()->createTape(cliId, tape);
  }
  qrr.m_vid = tape2;
  scheduler.queueRepack(cliId, qrr, lc);
  {
    auto repacks = scheduler.getRepacks();
    ASSERT_EQ(1, repacks.size());
    auto repack = scheduler.getRepack(repacks.front().vid);
    ASSERT_EQ(tape2, repack.vid);
  }
  scheduler.promoteRepackRequestsToToExpand(lc,2);
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  common::dataStructures::SecurityIdentity cliId;
  cliId.host = "host";
  cliId.username = s_userName;
  std::string tape1 = "TAPE";
  {
    auto tape = getDefaultTape();
    tape.vid = tape1;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(cliId, tape);
  }

  //Queue the first repack request
  cta::SchedulerDatabase::QueueRepackRequest qrr(tape1,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
  scheduler.queueRepack(cliId, qrr, lc);

  std::string tape2 = "TAPE2";

  {
    auto tape = getDefaultTape();
    tape.vid = tape2;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(cliId, tape);
  }

  //Queue the second repack request
  qrr.m_vid = tape2;
  qrr.m_repackType = common::dataStructures::RepackInfo::Type::AddCopiesOnly;
  scheduler.queueRepack(cliId,qrr,lc);

  //Test the repack request queued has status Pending
  ASSERT_EQ(scheduler.getRepack(tape1).status,common::dataStructures::RepackInfo::Status::Pending);
  ASSERT_EQ(scheduler.getRepack(tape2).status,common::dataStructures::RepackInfo::Status::Pending);

  //Change the repack request status to ToExpand
  scheduler.promoteRepackRequestsToToExpand(lc,2);

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
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;

  int ctrl_var = 0;
  while (ctrl_var == 0) usleep(1000000);

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();
  auto &schedulerDB = getSchedulerDB();

  setupDefaultCatalogue();
  catalogue.DiskInstance()->createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.DiskInstanceSpace()->createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "constantFreeSpace:10", 10, "no comment");
  catalogue.DiskSystem()->createDiskSystem({"user", "host"}, "diskSystem", "diskInstance", "diskInstanceSpace", "/public_dir/public_file", 10L*1000*1000*1000, 15*60, "no comment");


#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  //Create an agent to represent this test process
  std::string agentReferenceName = "expandRepackRequestTest";
  std::unique_ptr<objectstore::AgentReference> agentReference(new objectstore::AgentReference(agentReferenceName, dl));


  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  uint64_t nbTapesToRepack = 10;
  uint64_t nbTapesForTest = 2; //corresponds to the targetAvailableRequests variable in the Scheduler::promoteRepackRequestsToToExpand() method

  std::vector<std::string> allVid;

  //Create the tapes from which we will retrieve
  for(uint64_t i = 1; i <= nbTapesToRepack ; ++i){
    std::ostringstream ossVid;
    ossVid << s_vid << "_" << i;
    std::string vid = ossVid.str();
    allVid.push_back(vid);

    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
    for(uint64_t i = 1; i<= nbTapesToRepack;++i){
      std::string currentVid = allVid.at(i-1);
      for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
        std::ostringstream diskFileId;
        diskFileId << (12345677 + archiveFileId);
        std::ostringstream diskFilePath;
        diskFilePath << "/public_dir/public_file_"<<i<<"_"<< j;
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
        fileWritten.vid = currentVid;
        fileWritten.fSeq = j;
        fileWritten.blockId = j * 100;
        fileWritten.copyNb = 1;
        fileWritten.tapeDrive = tapeDrive;
        tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
      }
      //update the DB tape
      catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
      tapeFilesWrittenCopy1.clear();
    }
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    for(uint64_t i = 0; i < nbTapesToRepack ; ++i) {
      //Queue the first repack request
      cta::SchedulerDatabase::QueueRepackRequest qrr(allVid.at(i),"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
        common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
      scheduler.queueRepack(admin,qrr,lc);
    }
    scheduler.waitSchedulerDbSubthreadsComplete();
    //scheduler.waitSchedulerDbSubthreadsComplete();
    for(uint64_t i = 0; i < nbTapesToRepack;++i){
      log::TimingList tl;
      utils::Timer t;
      if(i % nbTapesForTest == 0){
        //The promoteRepackRequestsToToExpand will only promote 2 RepackRequests to ToExpand status at a time.
        scheduler.promoteRepackRequestsToToExpand(lc,2);
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
        ASSERT_EQ(retrieveJob.fileSize,archiveFileSize);
        std::stringstream ss;
        ss<<"file://"<<tempDirectory.path()<<"/"<<allVid.at(i-1)<<"/"<<std::setw(9)<<std::setfill('0')<<j;
        ASSERT_EQ(retrieveJob.request.dstURL, ss.str());
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.copyNb,1);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.checksumBlob,checksumBlob);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.blockId,j*100);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.fileSize,archiveFileSize);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.fSeq,j);
        ASSERT_EQ(retrieveJob.tapeCopies[vid].second.vid,vid);
        ++j;
      }
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  const std::string driveName = "tape_drive";

  //Now, we need to simulate a retrieve for each file
  {
    // Emulate a tape server by asking for nbTapesForTest mount and then all files
    for(uint64_t i = 1; i<= nbTapesForTest ;++i)
    {
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
        rrp.reportCompletedJob(std::move(*it), lc);
      }
      rrp.setDiskDone();
      rrp.setTapeDone();

      rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

      rrp.reportEndOfSession(lc);
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
        std::string retrieveQueueToReportToRepackForSuccessAddress = re.getRetrieveQueueAddress(ri.getRepackRequestAddress(allVid.at(i-1)), JobQueueType::JobsToReportToRepackForSuccess);
        cta::objectstore::RetrieveQueue rq(retrieveQueueToReportToRepackForSuccessAddress,schedulerDB.getBackend());


        while (ctrl_var == 0) usleep(1000000);

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
          common::dataStructures::TapeFile tapeFile = retrieveRequest.getArchiveFile().tapeFiles.at(copyNb);
          common::dataStructures::RetrieveRequest schedulerRetrieveRequest = retrieveRequest.getSchedulerRequest();
          common::dataStructures::ArchiveFile archiveFile = retrieveRequest.getArchiveFile();

          //Testing tape file
          ASSERT_EQ(tapeFile.vid,allVid.at(i-1));
          ASSERT_EQ(tapeFile.blockId,j * 100);
          ASSERT_EQ(tapeFile.fSeq,j);
          ASSERT_EQ(tapeFile.checksumBlob, checksumBlob);
          ASSERT_EQ(tapeFile.fileSize, archiveFileSize);

          //Testing scheduler retrieve request
          ASSERT_EQ(schedulerRetrieveRequest.archiveFileID,archiveFileId++);
          std::stringstream ss;
          ss<<"file://"<<tempDirectory.path()<<"/"<<allVid.at(i-1)<<"/"<<std::setw(9)<<std::setfill('0')<<j;
          ASSERT_EQ(schedulerRetrieveRequest.dstURL,ss.str());
          //Testing the retrieve request
          ASSERT_EQ(retrieveRequest.getRepackInfo().isRepack,true);
          ASSERT_EQ(retrieveRequest.getQueueType(),JobQueueType::JobsToReportToRepackForSuccess);
          ASSERT_EQ(retrieveRequest.getRetrieveFileQueueCriteria().mountPolicy,cta::common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack);
          ASSERT_EQ(retrieveRequest.getActiveCopyNumber(),1);
          ASSERT_EQ(retrieveRequest.getJobStatus(job.copyNb),cta::objectstore::serializers::RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
          ASSERT_EQ(retrieveRequest.getJobs().size(),1);

          //Testing the archive file associated to the retrieve request
          ASSERT_EQ(archiveFile.storageClass,storageClass.name);
          ASSERT_EQ(archiveFile.diskInstance,s_diskInstance);
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
      for (auto queueType: {JobQueueType::FailedJobs,
        JobQueueType::JobsToReportToRepackForFailure,
        JobQueueType::JobsToReportToRepackForSuccess,
        JobQueueType::JobsToReportToUser,
        JobQueueType::JobsToTransferForRepack,
        JobQueueType::JobsToTransferForUser}) {
        ASSERT_EQ(0, re.dumpRetrieveQueues(queueType).size());
      }
      ASSERT_EQ(1, re.dumpArchiveQueues(JobQueueType::JobsToTransferForRepack).size());
      for (auto queueType: {JobQueueType::FailedJobs,
        JobQueueType::JobsToReportToRepackForFailure,
        JobQueueType::JobsToReportToRepackForSuccess,
        JobQueueType::JobsToReportToUser,
        JobQueueType::JobsToTransferForUser}) {
        ASSERT_EQ(0, re.dumpArchiveQueues(queueType).size());
      }
      // Now check we find all our requests in the archive queue.
      cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(s_tapePoolName, JobQueueType::JobsToTransferForRepack),
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
        ASSERT_EQ(archiveFile.checksumBlob,checksumBlob);
        std::ostringstream diskFileId;
        diskFileId << (12345677 + archiveFile.archiveFileID);
        ASSERT_EQ(archiveFile.diskFileId,diskFileId.str());
        ASSERT_EQ(archiveFile.diskFileInfo.gid,PUBLIC_GID);
        ASSERT_EQ(archiveFile.diskFileInfo.owner_uid,PUBLIC_OWNER_UID);
        ASSERT_EQ(archiveFile.fileSize,archiveFileSize);
        ASSERT_EQ(archiveFile.storageClass,s_storageClassName);
        std::stringstream ss;
        ss<<"file://"<<tempDirectory.path()<<"/"<<allVid.at(tapeIndex-1)<<"/"<<std::setw(9)<<std::setfill('0')<<fileIndex;
        ASSERT_EQ(ar.getSrcURL(),ss.str());
        for(auto archiveJob : ar.dumpJobs()){
          ASSERT_EQ(archiveJob.status,cta::objectstore::serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
          ASSERT_EQ(aq.getAddressIfSet(), archiveJob.owner);
        }
        archiveIdsSeen.insert(archiveFile.archiveFileID);
      }
      // Validate we got all the files we expected.
      const uint8_t NUMBER_OF_IDS = 20;
      ASSERT_EQ(NUMBER_OF_IDS, archiveIdsSeen.size());
      for (uint8_t id = 1; id <= NUMBER_OF_IDS; id++) {
        ASSERT_EQ(1, archiveIdsSeen.count(id));
      }
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestRetrieveFailed) {
  using namespace cta;
  using namespace cta::objectstore;
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
        common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    //The promoteRepackRequestsToToExpand will only promote 2 RepackRequests to ToExpand status at a time.
    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    //If we have expanded 2 repack requests, the getNextRepackRequestToExpand will return null as it is not possible
    //to promote more than 2 repack requests at a time. So we break here.

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  const std::string driveName = "tape_drive";
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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
      rrp.reportCompletedJob(std::move(*it), lc);
      it++;
    }
    std::unique_ptr<cta::RetrieveJob> failedJobUniqPtr = std::move(*(executedJobs.begin()));
    rrp.reportFailedJob(std::move(failedJobUniqPtr),cta::exception::Exception("FailedJob expandRepackRequestFailedRetrieve"), lc);

    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

    rrp.reportEndOfSession(lc);
    rrp.waitThread();

    ASSERT_EQ(rrp.allThreadsDone(),true);

    scheduler.waitSchedulerDbSubthreadsComplete();

    {
      //Verify that the job is in the RetrieveQueueToReportToRepackForFailure
      cta::objectstore::RootEntry re(backend);
      cta::objectstore::ScopedExclusiveLock sel(re);
      re.fetch();

      //Get the retrieveQueueToReportToRepackForFailure
      // The queue is named after the repack request: we need to query the repack index
      objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
      ri.fetchNoLock();

      std::string retrieveQueueToReportToRepackForFailureAddress = re.getRetrieveQueueAddress(ri.getRepackRequestAddress(vid),JobQueueType::JobsToReportToRepackForFailure);
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

      ASSERT_THROW(re.getRetrieveQueueAddress(ri.getRepackRequestAddress(vid),JobQueueType::JobsToReportToRepackForFailure),cta::exception::Exception);
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestArchiveSuccess) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a repack destination tape
  std::string vidDestination = "VIDDESTINATION";

  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
    //scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    //The promoteRepackRequestsToToExpand will only promote 2 RepackRequests to ToExpand status at a time.
    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    //If we have expanded 2 repack requests, the getNextRepackRequestToExpand will return null as it is not possible
    //to promote more than 2 repack requests at a time. So we break here.

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  const std::string driveName = "tape_drive";
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

    //Report all jobs as succeeded
    for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
    {
      rrp.reportCompletedJob(std::move(*it), lc);
    }

    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

    rrp.reportEndOfSession(lc);
    rrp.waitThread();

    ASSERT_TRUE(rrp.allThreadsDone());
  }
  {
    //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
    while (true) {
      auto rep = schedulerDB.getNextRepackReportBatch(lc);
      if (nullptr == rep) break;
      rep->report(lc);
    }
  }
  //All retrieve have been successfully executed, let's get all the ArchiveJobs generated from the succeeded RetrieveJobs of Repack
  {
    scheduler.waitSchedulerDbSubthreadsComplete();
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::unique_ptr<cta::ArchiveJob> archiveJob;

    //Get all Archive jobs
    std::list<std::unique_ptr<cta::ArchiveJob>> executedJobs;
    for(uint64_t j = 1;j<=nbArchiveFilesPerTape;++j){
      auto jobBatch = archiveMount->getNextJobBatch(1,archiveFileSize,lc);
      archiveJob.reset(jobBatch.front().release());
      archiveJob->tapeFile.blockId = j * 101;
      archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      archiveJob->tapeFile.fileSize = archiveFileSize;
      ASSERT_NE(nullptr,archiveJob.get());
      executedJobs.push_back(std::move(archiveJob));
    }

    castor::tape::tapeserver::daemon::MigrationReportPacker mrp(archiveMount.get(),lc);
    mrp.startThreads();

    //Report all archive jobs as succeeded
    for(auto it = executedJobs.begin();it != executedJobs.end(); ++it){
      mrp.reportCompletedJob(std::move(*it),lc);
    }

    castor::tape::tapeserver::drive::compressionStats compressStats;
    mrp.reportFlush(compressStats,lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread();

    //Check that the ArchiveRequests are in the ArchiveQueueToReportToRepackForSuccess
    {
      //Verify that the job is in the ArchiveQueueToReportToRepackForSuccess
      cta::objectstore::RootEntry re(backend);
      cta::objectstore::ScopedExclusiveLock sel(re);
      re.fetch();

      // Get the ArchiveQueueToReportToRepackForSuccess
      // The queue is named after the repack request: we need to query the repack index
      objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
      ri.fetchNoLock();

      using cta::common::dataStructures::JobQueueType;
      std::string archiveQueueToReportToRepackForSuccessAddress = re.getArchiveQueueAddress(ri.getRepackRequestAddress(vid),JobQueueType::JobsToReportToRepackForSuccess);
      cta::objectstore::ArchiveQueue aq(archiveQueueToReportToRepackForSuccessAddress,backend);

      //Fetch the queue so that we can get the retrieveRequests from it
      cta::objectstore::ScopedExclusiveLock aql(aq);
      aq.fetch();

      ASSERT_EQ(aq.dumpJobs().size(),10);
      for(auto& job: aq.dumpJobs()){
        ASSERT_EQ(1,job.copyNb);
        ASSERT_EQ(archiveFileSize,job.size);
      }
    }
    {
      //Do the reporting of the Archive Jobs succeeded
      Scheduler::RepackReportBatch reports = scheduler.getNextRepackReportBatch(lc);
      reports.report(lc);
      scheduler.waitSchedulerDbSubthreadsComplete();
    }
    {
      //Test that the repackRequestStatus is set as Complete.
      cta::objectstore::RootEntry re(backend);
      cta::objectstore::ScopedExclusiveLock sel(re);
      re.fetch();
      objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
      ri.fetchNoLock();
      cta::objectstore::RepackRequest rr(ri.getRepackRequestAddress(vid),backend);
      rr.fetchNoLock();
      ASSERT_EQ(vidDestination,rr.getRepackDestinationInfos().front().vid);
      ASSERT_EQ(nbArchiveFilesPerTape,rr.getRepackDestinationInfos().front().files);
      ASSERT_EQ(nbArchiveFilesPerTape * archiveFileSize,rr.getRepackDestinationInfos().front().bytes);
      ASSERT_EQ(common::dataStructures::RepackInfo::Status::Complete,rr.getInfo().status);
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestArchiveFailed) {
  using namespace cta;
  using namespace cta::objectstore;
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a repack destination tape
  std::string vidDestinationRepack = "VIDDESTINATIONREPACK";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestinationRepack;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  const std::string driveName = "tape_drive";
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

    //Report all jobs as succeeded
    for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
    {
      rrp.reportCompletedJob(std::move(*it), lc);
    }

    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

    rrp.reportEndOfSession(lc);
    rrp.waitThread();

    ASSERT_TRUE(rrp.allThreadsDone());
  }
  {
    //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
    while (true) {
      auto rep = schedulerDB.getNextRepackReportBatch(lc);
      if (nullptr == rep) break;
      rep->report(lc);
    }
  }
  //All retrieve have been successfully executed, let's get all the ArchiveJobs generated from the succeeded RetrieveJobs of Repack
  {
    scheduler.waitSchedulerDbSubthreadsComplete();
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

    std::unique_ptr<cta::ArchiveMount> archiveMount;
    archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
    ASSERT_NE(nullptr, archiveMount.get());
    std::unique_ptr<cta::ArchiveJob> archiveJob;

    //Get all Archive jobs
    std::list<std::unique_ptr<cta::ArchiveJob>> executedJobs;
    for(uint64_t j = 1;j<=nbArchiveFilesPerTape;++j){
      auto jobBatch = archiveMount->getNextJobBatch(1,archiveFileSize,lc);
      archiveJob.reset(jobBatch.front().release());
      archiveJob->tapeFile.blockId = j * 101;
      archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      archiveJob->tapeFile.fileSize = archiveFileSize;
      ASSERT_NE(nullptr,archiveJob.get());
      executedJobs.push_back(std::move(archiveJob));
    }

    {
      castor::tape::tapeserver::daemon::MigrationReportPacker mrp(archiveMount.get(),lc);
      mrp.startThreads();

      //Report all archive jobs as succeeded except the first one
      auto it = executedJobs.begin();
      mrp.reportSkippedJob(std::move(*it),"expandRepackRequestFailedArchive",lc);
      it++;
      while(it != executedJobs.end()){
        mrp.reportCompletedJob(std::move(*it),lc);
        it++;
      }

      castor::tape::tapeserver::drive::compressionStats compressStats;
      mrp.reportFlush(compressStats,lc);
      mrp.reportEndOfSession(lc);
      mrp.reportTestGoingToEnd(lc);
      mrp.waitThread();

      {
        //Test only 9 jobs are in the ArchiveQueueToReportToRepackForSuccess
        cta::objectstore::RootEntry re(backend);
        re.fetchNoLock();
        objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
        ri.fetchNoLock();

        std::string archiveQueueToReportToRepackForSuccessAddress = re.getArchiveQueueAddress(ri.getRepackRequestAddress(vid),JobQueueType::JobsToReportToRepackForSuccess);
        cta::objectstore::ArchiveQueue aq(archiveQueueToReportToRepackForSuccessAddress,backend);

        aq.fetchNoLock();
        ASSERT_EQ(9,aq.dumpJobs().size());
      }
    }

    //Test that the failed job is queued in the ArchiveQueueToReportToRepackForFailure
    {
      cta::objectstore::RootEntry re(backend);
      re.fetchNoLock();
      objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
      ri.fetchNoLock();

      std::string archiveQueueToReportToRepackForFailureAddress = re.getArchiveQueueAddress(ri.getRepackRequestAddress(vid),JobQueueType::JobsToReportToRepackForFailure);
      cta::objectstore::ArchiveQueue aq(archiveQueueToReportToRepackForFailureAddress,backend);

      aq.fetchNoLock();

      for(auto &job: aq.dumpJobs()){
        ASSERT_EQ(1,job.copyNb);
        ASSERT_EQ(archiveFileSize,job.size);
      }
    }
    {
      //Do the reporting of the ArchiveJobs
      while (true) {
        auto rep = schedulerDB.getNextRepackReportBatch(lc);
        if (nullptr == rep) break;
        rep->report(lc);
      }
    }
    {
      scheduler.waitSchedulerDbSubthreadsComplete();
      //Test that the repackRequestStatus is set as Failed.
      cta::objectstore::RootEntry re(backend);
      cta::objectstore::ScopedExclusiveLock sel(re);
      re.fetch();
      objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
      ri.fetchNoLock();
      cta::objectstore::RepackRequest rr(ri.getRepackRequestAddress(vid),backend);
      rr.fetchNoLock();
      ASSERT_EQ(common::dataStructures::RepackInfo::Status::Failed,rr.getInfo().status);
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestRepackingTape) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in 1 tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  // Queue the repack request for a repacking tape
  // Should work
  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    ASSERT_NO_THROW(scheduler.queueRepack(admin,qrr,lc));
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    ASSERT_NE(nullptr,repackRequestToExpand);
  }
}

TEST_P(SchedulerTest, expandRepackRequestRepackingDisabledTape) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING_DISABLED;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in 1 tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  // Queue the repack request for a repacking tape
  // Should work
  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
                                                   common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    ASSERT_NO_THROW(scheduler.queueRepack(admin,qrr,lc));
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    ASSERT_NE(nullptr,repackRequestToExpand);
  }
}

TEST_P(SchedulerTest, expandRepackRequestBrokenTape) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::BROKEN;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    ASSERT_THROW(scheduler.queueRepack(admin,qrr,lc),cta::exception::UserError);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    ASSERT_EQ(nullptr,repackRequestToExpand);
  }
}

TEST_P(SchedulerTest, expandRepackRequestDisabledTape) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::DISABLED;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
                                                  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    ASSERT_THROW(scheduler.queueRepack(admin,qrr,lc),cta::exception::UserError);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    ASSERT_EQ(nullptr,repackRequestToExpand);
  }
}

TEST_P(SchedulerTest, expandRepackRequestActiveTape) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::ACTIVE;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
      common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    ASSERT_THROW(scheduler.queueRepack(admin,qrr,lc),cta::exception::UserError);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    ASSERT_EQ(nullptr,repackRequestToExpand);
  }
}

/*
TEST_P(SchedulerTest, noMountIsTriggeredWhenTapeIsDisabled) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in 1 tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the queueing of the Retrieve Request and try to mount after having disabled the tape
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    std::string diskInstance="disk_instance";
    cta::common::dataStructures::RetrieveRequest rReq;
    rReq.archiveFileID=1;
    rReq.requester.name = s_userName;
    rReq.requester.group = "someGroup";
    rReq.dstURL = "dst_url";
    scheduler.queueRetrieve(diskInstance, rReq, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  //disabled the tape

  std::string disabledReason = "Disabled reason";
  catalogue.Tape()->setTapeDisabled(admin,vid,disabledReason);
  const std::string driveName = "tape_drive";

  //No mount should be returned by getNextMount
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  //enable the tape
  catalogue.Tape()->modifyTapeState(admin, vid, common::dataStructures::Tape::ACTIVE, std::nullopt, std::nullopt);

  //A mount should be returned by getNextMount
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName,driveName,lc));

  //disable the tape
  catalogue.Tape()->setTapeDisabled(admin,vid,disabledReason);
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName,driveName,lc));

  //create repack mount policy
  const std::string mountPolicyName = s_repackMountPolicyName;
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

  catalogue.MountPolicy()->createMountPolicy(s_adminOnAdminHost, mountPolicy);

  auto mountPolicies = catalogue.MountPolicy()->getMountPolicies();

  auto mountPolicyItor = std::find_if(mountPolicies.begin(),mountPolicies.end(), [](const common::dataStructures::MountPolicy &mountPolicy){
        return mountPolicy.name.rfind("repack", 0) == 0;
  });

  ASSERT_NE(mountPolicyItor, mountPolicies.end());

  cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    *mountPolicyItor,s_defaultRepackNoRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  log::TimingList tl;
  utils::Timer t;

  scheduler.promoteRepackRequestsToToExpand(lc,2);
  scheduler.waitSchedulerDbSubthreadsComplete();

  auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();

  scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  //Test expected behaviour for NOW:
  //The getNextMount should return a mount as the tape is disabled, there are repack --disabledtape retrieve jobs in it
  //and the mount policy name begins with repack
  //We will then get the Repack AND USER jobs from the getNextJobBatch
  auto nextMount = scheduler.getNextMount(s_libraryName,driveName,lc);
  ASSERT_NE(nullptr,nextMount);
  std::unique_ptr<cta::RetrieveMount> retrieveMount;
  retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(nextMount.release()));
  auto jobBatch = retrieveMount->getNextJobBatch(20,20*archiveFileSize,lc);
  ASSERT_EQ(11,jobBatch.size()); //1 user job + 10 Repack jobs = 11 jobs in the batch
}*/
/*
TEST_P(SchedulerTest, emptyMountIsTriggeredWhenCancelledRetrieveRequest) {

  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();

  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in 1 tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the queueing of the Retrieve Request and try to mount after having disabled the tape
  scheduler.waitSchedulerDbSubthreadsComplete();
  uint64_t archiveFileId = 1;
  std::string dstUrl = "dst_url";
  std::string diskInstance="disk_instance";
  {
    cta::common::dataStructures::RetrieveRequest rReq;
    rReq.archiveFileID=1;
    rReq.requester.name = s_userName;
    rReq.requester.group = "someGroup";
    rReq.dstURL = dstUrl;
    scheduler.queueRetrieve(diskInstance, rReq, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  //disabled the tape
  std::string disabledReason = "reason";
  catalogue.Tape()->setTapeDisabled(admin,vid,disabledReason);

  //No mount should be returned by getNextMount
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  //abort the retrieve request
  {
    //Get the only retrieve job in the queue
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();
    std::string retrieveQueueAddr = re.getRetrieveQueueAddress(vid,JobQueueType::JobsToTransferForUser);
    cta::objectstore::RetrieveQueue retrieveQueue(retrieveQueueAddr,backend);
    retrieveQueue.fetchNoLock();
    std::string retrieveRequestAddr = retrieveQueue.dumpJobs().front().address;
    common::dataStructures::CancelRetrieveRequest crr;
    crr.retrieveRequestId = retrieveRequestAddr;
    crr.archiveFileID = archiveFileId;
    crr.dstURL = dstUrl;
    scheduler.abortRetrieve(diskInstance,crr,lc);
  }

  //A mount should be returned by getNextMount
  auto retrieveMount = scheduler.getNextMount(s_libraryName,driveName,lc);
  ASSERT_NE(nullptr,retrieveMount);

}
*/

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const std::string driveName = "tape_drive";
  for (auto i = 0; i < NUMBER_OF_FILES; i++) {
    auto tape = getDefaultTape();
    std::string vid = s_vid + std::to_string(i);
    tape.vid = vid;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
    catalogue.Tape()->tapeLabelled(vid, driveName);
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
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //Create the source tape
  std::string vid = "VIDSOURCE";
  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create two different destination tapepool
  std::string tapepool2Name = "tapepool2";
  const std::optional<std::string> supply;
  catalogue.TapePool()->createTapePool(admin,tapepool2Name,"vo",1,false,supply,"comment");

  std::string tapepool3Name = "tapepool3";
  catalogue.TapePool()->createTapePool(admin,tapepool3Name,"vo",1,false,supply,"comment");

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 3;
  storageClass.comment = "Create storage class";
  catalogue.StorageClass()->modifyStorageClassNbCopies(admin,storageClass.name,storageClass.nbCopies);

  //Create the two archive routes for the new copies
  catalogue.ArchiveRoute()->createArchiveRoute(admin,storageClass.name,2,tapepool2Name,"ArchiveRoute2");
  catalogue.ArchiveRoute()->createArchiveRoute(admin,storageClass.name,3,tapepool3Name,"ArchiveRoute3");

  //Create two other destinationTape
  std::string vidDestination1 = "VIDDESTINATION1";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination1;
    tape.tapePoolName = tapepool2Name;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  std::string vidDestination2 = "VIDDESTINATION2";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination2;
    tape.tapePoolName = tapepool3Name;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files the source tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expanding requeue the Repack after the creation of
  //one retrieve request
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::AddCopiesOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    //Get the address of the Repack Request
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();

    std::string repackQueueAddress = re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending);

    cta::objectstore::RepackQueuePending repackQueuePending(repackQueueAddress,backend);
    repackQueuePending.fetchNoLock();

    std::string repackRequestAddress = repackQueuePending.getCandidateList(1,{}).candidates.front().address;

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    //scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    ASSERT_EQ(vid,repackRequestToExpand->getRepackInfo().vid);

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);

    {
      cta::objectstore::RepackRequest rr(repackRequestAddress,backend);
      rr.fetchNoLock();
      //As storage class nbcopies = 3 and as the 10 files already archived have 1 copy in CTA,
      //The repack request should have 20 files to archive
      ASSERT_EQ(20,rr.getTotalStatsFile().totalFilesToArchive);
      ASSERT_EQ(20*archiveFileSize, rr.getTotalStatsFile().totalBytesToArchive);
      //The number of files to Retrieve remains the same
      ASSERT_EQ(10,rr.getTotalStatsFile().totalFilesToRetrieve);
      ASSERT_EQ(10*archiveFileSize,rr.getTotalStatsFile().totalBytesToRetrieve);
    }
  }
  const std::string driveName = "tape_drive";
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

    //Report all jobs as succeeded
    for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
    {
      rrp.reportCompletedJob(std::move(*it), lc);
    }

    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

    rrp.reportEndOfSession(lc);
    rrp.waitThread();

    ASSERT_TRUE(rrp.allThreadsDone());
  }
  {
    //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
    while (true) {
      auto rep = schedulerDB.getNextRepackReportBatch(lc);
      if (nullptr == rep) break;
      rep->report(lc);
    }
  }
  //All retrieve have been successfully executed, let's see if there are 2 mount for different vids with 10 files
  //per batch
  {
    scheduler.waitSchedulerDbSubthreadsComplete();
    {
      //The first mount given by the scheduler should be the vidDestination1 that belongs to the tapepool1
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());

      {
        auto jobBatch = archiveMount->getNextJobBatch(20,20 * archiveFileSize,lc);
        ASSERT_EQ(10,jobBatch.size());
        ASSERT_EQ(vidDestination1,archiveMount->getVid());
      }
    }

    {
      //Second mount should be the vidDestination2 that belongs to the tapepool2
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());

      {
        auto jobBatch = archiveMount->getNextJobBatch(20,20 * archiveFileSize,lc);
        ASSERT_EQ(10,jobBatch.size());
        ASSERT_EQ(vidDestination2,archiveMount->getVid());
      }
    }
  }
}

TEST_P(SchedulerTest, expandRepackRequestShouldFailIfArchiveRouteMissing) {
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //Create the source tape
  std::string vidCopyNb1 = "VIDSOURCE";
  {
    auto tape = getDefaultTape();
    tape.vid = vidCopyNb1;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create two different destination tapepool
  std::string tapepool2Name = "tapepool2";
  const std::optional<std::string> supply;
  catalogue.TapePool()->createTapePool(admin,tapepool2Name,"vo",1,false,supply,"comment");

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";
  catalogue.StorageClass()->modifyStorageClassNbCopies(admin,storageClass.name,storageClass.nbCopies);

  //Create the one archive route for the second copy
  catalogue.ArchiveRoute()->createArchiveRoute(admin,storageClass.name,2,tapepool2Name,"ArchiveRoute3");

  //Create two other destinationTape
  std::string vidCopyNb2_source = "VIDCOPYNB2_SOURCE";
  {
    auto tape = getDefaultTape();
    tape.vid = vidCopyNb2_source;
    tape.tapePoolName = tapepool2Name;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  std::string vidCopyNb2_destination = "VIDCOPYNB2_DESTINATION";
  {
    auto tape = getDefaultTape();
    tape.vid = vidCopyNb2_destination;
    tape.tapePoolName = tapepool2Name;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files the source tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vidCopyNb1;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy);
    tapeFilesWrittenCopy.clear();
  }
  //Archive the second copy of the files in the tape located in the tapepool2
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vidCopyNb2_source;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 2;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy);
    tapeFilesWrittenCopy.clear();
  }
  catalogue.Tape()->setTapeFull(admin,vidCopyNb2_source,true);
  //Delete the archive route of the second copy and repack the tape that contains these second copies
  catalogue.ArchiveRoute()->deleteArchiveRoute(storageClass.name,2);
  {
    std::string vid = vidCopyNb2_source;
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveAndAddCopies,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    //Get the address of the Repack Request
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();

    std::string repackQueueAddress = re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending);

    cta::objectstore::RepackQueuePending repackQueuePending(repackQueueAddress,backend);
    repackQueuePending.fetchNoLock();

    std::string repackRequestAddress = repackQueuePending.getCandidateList(1,{}).candidates.front().address;

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    scheduler.waitSchedulerDbSubthreadsComplete();

    ASSERT_EQ(vid,repackRequestToExpand->getRepackInfo().vid);

    //Expansion should fail.
    ASSERT_THROW(scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc),cta::ExpandRepackRequestException);
  }
}

TEST_P(SchedulerTest, expandRepackRequestMoveAndAddCopies){
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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
  agent.setTimeout_us(100);
  agent.insertAndRegisterSelf(lc);

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //Create the source tape
  std::string vid = "VIDSOURCE";
  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  //Create two different destination tapepool
  std::string tapepool2Name = "tapepool2";
  const std::optional<std::string> supply;
  catalogue.TapePool()->createTapePool(admin,tapepool2Name,"vo",1,false,supply,"comment");

  std::string tapepool3Name = "tapepool3";
  catalogue.TapePool()->createTapePool(admin,tapepool3Name,"vo",1,false,supply,"comment");

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 3;
  storageClass.comment = "Create storage class";
  catalogue.StorageClass()->modifyStorageClassNbCopies(admin,storageClass.name,storageClass.nbCopies);

  //Create the two archive routes for the new copies
  catalogue.ArchiveRoute()->createArchiveRoute(admin,storageClass.name,2,tapepool2Name,"ArchiveRoute2");
  catalogue.ArchiveRoute()->createArchiveRoute(admin,storageClass.name,3,tapepool3Name,"ArchiveRoute3");

  //Create two other destinationTape and one for the move workflow
  std::string vidDestination1 = "VIDDESTINATION1";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination1;
    tape.tapePoolName = tapepool2Name;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  std::string vidDestination2 = "VIDDESTINATION2";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination2;
    tape.tapePoolName = tapepool3Name;
    tape.full = false;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  std::string vidMove = "VIDMOVE";
  {
    auto tape = getDefaultTape();
    tape.vid = vidMove;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files the source tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expanding requeue the Repack after the creation of
  //one retrieve request
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveAndAddCopies,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    //Get the address of the Repack Request
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();

    std::string repackQueueAddress = re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending);

    cta::objectstore::RepackQueuePending repackQueuePending(repackQueueAddress,backend);
    repackQueuePending.fetchNoLock();

    std::string repackRequestAddress = repackQueuePending.getCandidateList(1,{}).candidates.front().address;

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();

    scheduler.waitSchedulerDbSubthreadsComplete();

    ASSERT_EQ(vid,repackRequestToExpand->getRepackInfo().vid);

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);

    {
      cta::objectstore::RepackRequest rr(repackRequestAddress,backend);
      rr.fetchNoLock();
      //As storage class nbcopies = 3 and as the 10 files already archived have 1 copy in CTA,
      //The repack request should have 20 files to archive
      ASSERT_EQ(30,rr.getTotalStatsFile().totalFilesToArchive);
      ASSERT_EQ(30*archiveFileSize, rr.getTotalStatsFile().totalBytesToArchive);
      //The number of files to Retrieve remains the same
      ASSERT_EQ(10,rr.getTotalStatsFile().totalFilesToRetrieve);
      ASSERT_EQ(10*archiveFileSize,rr.getTotalStatsFile().totalBytesToRetrieve);
    }
  }
  const std::string driveName = "tape_drive";
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
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

    //Report all jobs as succeeded
    for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
    {
      rrp.reportCompletedJob(std::move(*it), lc);
    }

    rrp.setDiskDone();
    rrp.setTapeDone();

    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

    rrp.reportEndOfSession(lc);
    rrp.waitThread();

    ASSERT_TRUE(rrp.allThreadsDone());
  }
  {
    //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
    while (true) {
      auto rep = schedulerDB.getNextRepackReportBatch(lc);
      if (nullptr == rep) break;
      rep->report(lc);
    }
  }
  //All retrieve have been successfully executed, let's see if there are 2 mount for different vids with 10 files
  //per batch
  {
    scheduler.waitSchedulerDbSubthreadsComplete();
    {
      //The first mount given by the scheduler should be the vidMove that belongs to the TapePool tapepool
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());

      {
        auto jobBatch = archiveMount->getNextJobBatch(20,20 * archiveFileSize,lc);
        ASSERT_EQ(10,jobBatch.size());
        ASSERT_EQ(vidMove,archiveMount->getVid());
      }
    }

    {
      //Second mount should be the vidDestination1 that belongs to the tapepool
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());

      {
        auto jobBatch = archiveMount->getNextJobBatch(20,20 * archiveFileSize,lc);
        ASSERT_EQ(10,jobBatch.size());
        ASSERT_EQ(vidDestination1,archiveMount->getVid());
      }
    }

    {
      //Third mount should be the vidDestination2 that belongs to the same tapepool as the repacked tape
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      ASSERT_NE(nullptr, mount.get());
      ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack, mount.get()->getMountType());

      std::unique_ptr<cta::ArchiveMount> archiveMount;
      archiveMount.reset(dynamic_cast<cta::ArchiveMount*>(mount.release()));
      ASSERT_NE(nullptr, archiveMount.get());

      {
        auto jobBatch = archiveMount->getNextJobBatch(20,20 * archiveFileSize,lc);
        ASSERT_EQ(10,jobBatch.size());
        ASSERT_EQ(vidDestination2,archiveMount->getVid());
      }
    }
  }
}

TEST_P(SchedulerTest, cancelRepackRequest) {
  using namespace cta;
  using namespace cta::objectstore;
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;
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

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  std::ostringstream ossVid;
  ossVid << s_vid << "_" << 1;
  std::string vid = ossVid.str();
  {
    auto tape = getDefaultTape();
    tape.vid = vid;
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }
  //Create a repack destination tape
  std::string vidDestination = "VIDDESTINATION";
  {
    auto tape = getDefaultTape();
    tape.vid = vidDestination;
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the expandRepackRequest method
  scheduler.waitSchedulerDbSubthreadsComplete();

  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  {
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();

    std::string repackQueueAddress = re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending);

    cta::objectstore::RepackQueuePending repackQueuePending(repackQueueAddress,backend);
    repackQueuePending.fetchNoLock();

    std::string repackRequestAddress = repackQueuePending.getCandidateList(1,{}).candidates.front().address;

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();

    scheduler.waitSchedulerDbSubthreadsComplete();

    ASSERT_EQ(vid,repackRequestToExpand->getRepackInfo().vid);

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);

    scheduler.waitSchedulerDbSubthreadsComplete();
    re.fetchNoLock();
    //Get all retrieve subrequests in the RetrieveQueue
    cta::objectstore::RetrieveQueue rq(re.getRetrieveQueueAddress(vid, JobQueueType::JobsToTransferForUser),backend);
    rq.fetchNoLock();
    for(auto & job: rq.dumpJobs()){
      //Check that subrequests exist in the objectstore
      cta::objectstore::RetrieveRequest retrieveReq(job.address,backend);
      ASSERT_NO_THROW(retrieveReq.fetchNoLock());
    }
    scheduler.cancelRepack(s_adminOnAdminHost,vid,lc);
    //Check that the subrequests are deleted from the objectstore
    for(auto & job: rq.dumpJobs()){
      cta::objectstore::RetrieveRequest retrieveReq(job.address,backend);
      ASSERT_THROW(retrieveReq.fetchNoLock(),cta::exception::NoSuchObject);
    }
    //Check that the RepackRequest is deleted from the objectstore
    ASSERT_THROW(cta::objectstore::RepackRequest(repackRequestAddress,backend).fetchNoLock(),cta::exception::NoSuchObject);
  }
  //Do another test to check the deletion of ArchiveSubrequests
  {
    cta::SchedulerDatabase::QueueRepackRequest qrr(vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
    common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
    scheduler.queueRepack(admin,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  {
    cta::objectstore::RootEntry re(backend);
    re.fetchNoLock();

    std::string repackQueueAddress = re.getRepackQueueAddress(common::dataStructures::RepackQueueType::Pending);

    cta::objectstore::RepackQueuePending repackQueuePending(repackQueueAddress,backend);
    repackQueuePending.fetchNoLock();

    std::string repackRequestAddress = repackQueuePending.getCandidateList(1,{}).candidates.front().address;

    log::TimingList tl;
    utils::Timer t;

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();

    scheduler.waitSchedulerDbSubthreadsComplete();

    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);

    scheduler.waitSchedulerDbSubthreadsComplete();

    const std::string driveName = "tape_drive";
    {
      std::unique_ptr<cta::TapeMount> mount;
      mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
      std::unique_ptr<cta::RetrieveMount> retrieveMount;
      retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
      std::unique_ptr<cta::RetrieveJob> retrieveJob;

      std::list<std::unique_ptr<cta::RetrieveJob>> executedJobs;
      //For each tape we will see if the retrieve jobs are not null
      for(uint64_t j = 1; j<=nbArchiveFilesPerTape; ++j)
      {
        auto jobBatch = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);
        retrieveJob.reset(jobBatch.front().release());
        executedJobs.push_back(std::move(retrieveJob));
      }
      //Now, report the retrieve jobs to be completed
      castor::tape::tapeserver::daemon::RecallReportPacker rrp(retrieveMount.get(),lc);

      rrp.startThreads();

      //Report all jobs as succeeded
      for(auto it = executedJobs.begin(); it != executedJobs.end(); ++it)
      {
        rrp.reportCompletedJob(std::move(*it), lc);
      }

      rrp.setDiskDone();
      rrp.setTapeDone();

      rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);

      rrp.reportEndOfSession(lc);
      rrp.waitThread();
    }
    {
      //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
      while (true) {
        auto rep = schedulerDB.getNextRepackReportBatch(lc);
        if (nullptr == rep) break;
        rep->report(lc);
      }
    }
    re.fetchNoLock();
    //Get all archive subrequests in the ArchiveQueue
    cta::objectstore::ArchiveQueue aq(re.getArchiveQueueAddress(s_tapePoolName, JobQueueType::JobsToTransferForRepack),backend);
    aq.fetchNoLock();
    for(auto & job: aq.dumpJobs()){
      cta::objectstore::ArchiveRequest archiveReq(job.address,backend);
      ASSERT_NO_THROW(archiveReq.fetchNoLock());
    }
    scheduler.cancelRepack(s_adminOnAdminHost,vid,lc);
    //Check that the subrequests are deleted from the objectstore
    for(auto & job: aq.dumpJobs()){
      cta::objectstore::ArchiveRequest archiveReq(job.address,backend);
      ASSERT_THROW(archiveReq.fetchNoLock(),cta::exception::NoSuchObject);
    }
    //Check that the RepackRequest is deleted from the objectstore
    ASSERT_THROW(cta::objectstore::RepackRequest(repackRequestAddress,backend).fetchNoLock(),cta::exception::NoSuchObject);
  }
}

TEST_P(SchedulerTest, getNextMountEmptyArchiveForRepackIfNbFilesQueuedIsLessThan2TimesMinFilesWarrantAMount) {
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

  //Create environment for the test
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,10000);

  Sorter sorter(agentReference,backend,catalogue);
  for(uint64_t i = 0; i < s_minFilesToWarrantAMount; ++i) {
    std::shared_ptr<cta::objectstore::ArchiveRequest> ar(new cta::objectstore::ArchiveRequest(agentReference.nextId("RepackSubRequest"),backend));
    ar->initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = i;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = 667;
    aFile.storageClass = "sc";
    ar->setArchiveFile(aFile);
    ar->addJob(1, s_tapePoolName, agentReference.getAgentAddress(), 1, 1, 1);
    ar->setJobStatus(1, serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
    cta::common::dataStructures::MountPolicy mp;
    mp.archiveMinRequestAge = 250000;
    ar->setMountPolicy(mp);
    ar->setArchiveReportURL("");
    ar->setArchiveErrorReportURL("");
    ar->setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar->setSrcURL("root://eoseos/myFile");
    ar->setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    sorter.insertArchiveRequest(ar, agentReference, lc);
    ar->insert();
  }

  sorter.flushAll(lc);

  const std::string driveName = "tape_drive";

  //As the scheduler minFilesToWarrantAMount is 5 and there is 5 ArchiveForRepack jobs queued
  //the call to getNextMount should return a nullptr (10 files mini to have an ArchiveForRepack mount)
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName,driveName,lc));

  for(uint64_t i = s_minFilesToWarrantAMount; i < 2 * s_minFilesToWarrantAMount; ++i) {
    std::shared_ptr<cta::objectstore::ArchiveRequest> ar(new cta::objectstore::ArchiveRequest(agentReference.nextId("RepackSubRequest"),backend));
    ar->initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = i;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = s_diskInstance;
    aFile.fileSize = 667;
    aFile.storageClass = s_storageClassName;
    ar->setArchiveFile(aFile);
    ar->addJob(1, s_tapePoolName, agentReference.getAgentAddress(), 1, 1, 1);
    ar->setJobStatus(1, serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
    cta::common::dataStructures::MountPolicy mp;
    mp.archiveMinRequestAge = 250000;
    ar->setMountPolicy(mp);
    ar->setArchiveReportURL("");
    ar->setArchiveErrorReportURL("");
    ar->setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar->setSrcURL("root://eoseos/myFile");
    ar->setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    sorter.insertArchiveRequest(ar, agentReference, lc);
    ar->insert();
  }

  sorter.flushAll(lc);

  //As there is now 10 files in the queue, the getNextMount method should return an ArchiveMount
  //with 10 files in it
  std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,driveName,lc);
  ASSERT_NE(nullptr,tapeMount);
  cta::ArchiveMount * archiveMount = dynamic_cast<cta::ArchiveMount *>(tapeMount.get());
  archiveMount->getNextJobBatch(2 * s_minFilesToWarrantAMount,2 * s_minBytesToWarrantAMount, lc);
  ASSERT_EQ(2 * s_minFilesToWarrantAMount,tapeMount->getNbFiles());
}

TEST_P(SchedulerTest, getNextMountTapeStatesThatShouldNotReturnAMount) {
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  auto tape = getDefaultTape();
  {
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(s_vid, driveName);

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

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::BROKEN,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::BROKEN,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::EXPORTED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::EXPORTED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::REPACKING_DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::REPACKING_DISABLED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::DISABLED,std::nullopt);
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
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::BROKEN,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::BROKEN,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::EXPORTED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::EXPORTED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::REPACKING_DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::REPACKING_DISABLED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));

  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::DISABLED,std::nullopt,std::string("Test"));
  ASSERT_EQ(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape.vid,common::dataStructures::Tape::ACTIVE,common::dataStructures::Tape::DISABLED,std::nullopt);
  ASSERT_NE(nullptr,scheduler.getNextMount(s_libraryName, driveName, lc));
}

TEST_P(SchedulerTest, repackRetrieveRequestsFailToFetchDiskSystem){
  using namespace cta;
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();
  //auto &schedulerDB = getSchedulerDB();
  //cta::objectstore::Backend& backend = schedulerDB.getBackend();

  setupDefaultCatalogue();
  catalogue.DiskInstance()->createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.DiskInstanceSpace()->createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "eos:ctaeos:default", 10, "no comment");
  catalogue.DiskSystem()->createDiskSystem({"user", "host"}, "repackBuffer", "diskInstance", "diskInstanceSpace", tempDirectory.path(), 10L*1000*1000*1000, 15*60, "no comment");

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  //Create an agent to represent this test process
  std::string agentReferenceName = "expandRepackRequestTest";
  std::unique_ptr<objectstore::AgentReference> agentReference(new objectstore::AgentReference(agentReferenceName, dl));

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  {
    auto tape = getDefaultTape();
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  cta::SchedulerDatabase::QueueRepackRequest qrr(s_vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,s_defaultRepackNoRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  scheduler.promoteRepackRequestsToToExpand(lc,2);
  scheduler.waitSchedulerDbSubthreadsComplete();
  auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
  log::TimingList tl;
  utils::Timer t;
  scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  const std::string driveName = "tape_drive";

  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, driveName, lc).release());
    ASSERT_NE(nullptr, mount.get());
    ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve, mount.get()->getMountType());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));
    ASSERT_NE(nullptr, retrieveMount.get());
    auto jobBatch = retrieveMount->getNextJobBatch(nbArchiveFilesPerTape,archiveFileSize * nbArchiveFilesPerTape,lc);
    cta::DiskSpaceReservationRequest reservationRequest;
    for(auto &job: jobBatch) {
      reservationRequest.addRequest(job->diskSystemName().value(), job->archiveFile.fileSize);
    }
    ASSERT_EQ(10,jobBatch.size());
    auto diskSpaceReservedBefore = catalogue.DriveState()->getTapeDrive(driveName).value().reservedBytes;
    //Trying to reserve disk space should result in 10 jobs should fail
    ASSERT_FALSE(retrieveMount->reserveDiskSpace(reservationRequest, lc));
    //No extra disk space was reserved
    auto diskSpaceReservedAfter = catalogue.DriveState()->getTapeDrive(driveName).value().reservedBytes;
    ASSERT_EQ(diskSpaceReservedAfter, diskSpaceReservedBefore);
  }
  /*
  // jobs are no longer requeued (this is now the responsibility of the retrieve mount)
  {
    //The jobs should be queued in the RetrieveQueueToReportToRepackForFailure
    cta::objectstore::RootEntry re(backend);
    cta::objectstore::ScopedExclusiveLock sel(re);
    re.fetch();

    // Get the retrieveQueueToReportToRepackForFailure
    // The queue is named after the repack request: we need to query the repack index
    objectstore::RepackIndex ri(re.getRepackIndexAddress(), schedulerDB.getBackend());
    ri.fetchNoLock();

    std::string retrieveQueueToReportToRepackForFailureAddress = re.getRetrieveQueueAddress(ri.getRepackRequestAddress(s_vid),JobQueueType::JobsToReportToRepackForFailure);
    cta::objectstore::RetrieveQueue rq(retrieveQueueToReportToRepackForFailureAddress,backend);

    //Fetch the queue so that we can get the retrieve jobs from it
    cta::objectstore::ScopedExclusiveLock rql(rq);
    rq.fetch();

    ASSERT_EQ(rq.dumpJobs().size(),10);
    for(auto& job: rq.dumpJobs()){
      ASSERT_EQ(1,job.copyNb);
      ASSERT_EQ(archiveFileSize,job.size);
    }
  }
  */
}

TEST_P(SchedulerTest, expandRepackRequestShouldThrowIfUseBufferNotRecallButNoDirectoryCreated){
  using namespace cta;
  unitTests::TempDirectory tempDirectory;

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();

  setupDefaultCatalogue();
  catalogue.DiskInstance()->createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.DiskInstanceSpace()->createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "eos:ctaeos:default", 10, "no comment");
  catalogue.DiskSystem()->createDiskSystem({"user", "host"}, "repackBuffer", "diskInstance", "diskInstanceSpace", tempDirectory.path(), 10L*1000*1000*1000, 15*60, "no comment");

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  //Create an agent to represent this test process
  std::string agentReferenceName = "expandRepackRequestTest";
  std::unique_ptr<objectstore::AgentReference> agentReference(new objectstore::AgentReference(agentReferenceName, dl));

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  {
    auto tape = getDefaultTape();
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  bool noRecall = true;

  cta::SchedulerDatabase::QueueRepackRequest qrr(s_vid,"file://DOES_NOT_EXIST",common::dataStructures::RepackInfo::Type::MoveOnly,
  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,noRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  scheduler.promoteRepackRequestsToToExpand(lc,2);
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
  catalogue.DiskInstance()->createDiskInstance({"user", "host"}, "diskInstance", "no comment");
  catalogue.DiskInstanceSpace()->createDiskInstanceSpace({"user", "host"}, "diskInstanceSpace", "diskInstance", "eos:ctaeos:default", 10, "no comment");
  catalogue.DiskSystem()->createDiskSystem({"user", "host"}, "repackBuffer", "diskInstance", "diskInstanceSpace",tempDirectory.path(), 10L*1000*1000*1000, 15*60, "no comment");

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  //Create an agent to represent this test process
  std::string agentReferenceName = "expandRepackRequestTest";
  std::unique_ptr<objectstore::AgentReference> agentReference(new objectstore::AgentReference(agentReferenceName, dl));

  cta::common::dataStructures::SecurityIdentity admin;
  admin.username = "admin_user_name";
  admin.host = "admin_host";

  //Create a logical library in the catalogue
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(admin, s_libraryName, libraryIsDisabled, physicalLibraryName, "Create logical library");

  {
    auto tape = getDefaultTape();
    tape.full = true;
    tape.state = common::dataStructures::Tape::REPACKING;
    tape.stateReason = "Test";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
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
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  bool noRecall = true;
  std::string pathRepackBuffer = "file://"+tempDirectory.path();
  tempDirectory.append("/"+s_vid);
  tempDirectory.mkdir();
  cta::SchedulerDatabase::QueueRepackRequest qrr(s_vid,pathRepackBuffer,common::dataStructures::RepackInfo::Type::MoveOnly,
  common::dataStructures::MountPolicy::s_defaultMountPolicyForRepack,noRecall);
  scheduler.queueRepack(admin,qrr, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  scheduler.promoteRepackRequestsToToExpand(lc,2);
  scheduler.waitSchedulerDbSubthreadsComplete();
  auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
  log::TimingList tl;
  utils::Timer t;
  ASSERT_NO_THROW(scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc));
}

TEST_P(SchedulerTest, archiveUserQueueMaxDrivesVoInFlightChangeScheduleMount){
  // This test will try to schedule one ArchiveForUser.
  // The VOs (including default repack VO) writeMaxDrives will be changed to ensure that it works well.
  // This test emulates 1 tapeserver
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  auto tape = getDefaultTape();
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape);

  const std::string driveName = "tape_drive";

  catalogue.Tape()->tapeLabelled(s_vid, driveName);


  log::DummyLogger dl("", "");
  log::LogContext lc(dl);
  const uint64_t archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, request.storageClass,
      request.requester, lc);
  scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, request, lc);
  scheduler.waitSchedulerDbSubthreadsComplete();

  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,0);

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
    catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,1);
    //nextMount should be false as the VO write max drives is still 0 (only the default repack VO max drives was changed)
    ASSERT_FALSE(nextMount);
    catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,1);
    //Reset the VO write max drives to a positive number should give a new mount
    nextMount = scheduler.getNextMountDryRun(s_libraryName,driveName,lc);
    ASSERT_TRUE(nextMount);
  }
}

TEST_P(SchedulerTest, retrieveUserQueueMaxDrivesVoInFlightChangeScheduleMount)
{
  // This test will try to schedule one retrieve (forUser).
  // The VOs (including default repack VO) readMaxDrives will be changed to ensure that it works well.
  // This test emulates 1 tapeserver
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  auto tape = getDefaultTape();
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape);

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in 1 tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = s_vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Test the queueing of the Retrieve Request and try to mount after having changed the readMaxDrives of the VO
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    std::string diskInstance="disk_instance";
    cta::common::dataStructures::RetrieveRequest rReq;
    rReq.archiveFileID=1;
    rReq.requester.name = s_userName;
    rReq.requester.group = "someGroup";
    rReq.dstURL = "dst_url";
    scheduler.queueRetrieve(diskInstance, rReq, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_repack_vo,0);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_repack_vo,1);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,1);
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));
}

TEST_P(SchedulerTest, retrieveArchiveRepackQueueMaxDrivesVoInFlightChangeScheduleMount)
{
  // This test will try to schedule one retrieve (for repack) followed by one archive
  // (after converting the repack retrieve requests into repack archive requests)
  // The VOs (including default repack VO) readMaxDrives and writeMaxDrives will be changed to ensure that it works well.
  // This test emulates 1 tapeserver
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  std::string drive = "drive";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //This tape will contain files for triggering a repack retrieve
  auto tape1 = getDefaultTape();
  tape1.state = common::dataStructures::Tape::REPACKING;
  tape1.stateReason = "Test";
  tape1.full = true;
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape1);

  //Create a repack destination tape
  auto tape2 = getDefaultTape();
  tape2.vid = "REPACK_DESTINATION_VID";
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape2);

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in the first tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    std::string currentVid = s_vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + j);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = j;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // Schedule a repack request and expand it into retrieve sub-requests
  {
    cta::common::dataStructures::MountPolicy mp;
    mp.name = s_mountPolicyName;

    cta::SchedulerDatabase::QueueRepackRequest qrr(tape1.vid,"file://"+tempDirectory.path(),common::dataStructures::RepackInfo::Type::MoveOnly,
                                                   mp,s_defaultRepackNoRecall);
    scheduler.queueRepack(s_adminOnAdminHost,qrr,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();

    scheduler.promoteRepackRequestsToToExpand(lc,2);
    scheduler.waitSchedulerDbSubthreadsComplete();

    log::TimingList tl;
    utils::Timer t;
    auto repackRequestToExpand = scheduler.getNextRepackRequestToExpand();
    scheduler.expandRepackRequest(repackRequestToExpand,tl,t,lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);
  catalogue.MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);

  // ReadMaxDrives pre-defined value is 1
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting ReadMaxDrives to 0 means that no mount should be scheduled
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_repack_vo,0);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting User VO ReadMaxDrives to 1
  // Repack VO still has ReadMaxDrives as 0, so no repack mount should be scheduled
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,1);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting Repack VO ReadMaxDrives to 1
  // One repack mount can be scheduled
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_repack_vo,1);
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Simulate a mount
  {
    std::unique_ptr<cta::TapeMount> mount;
    mount.reset(scheduler.getNextMount(s_libraryName, tapeDrive, lc).release());
    std::unique_ptr<cta::RetrieveMount> retrieveMount;
    retrieveMount.reset(dynamic_cast<cta::RetrieveMount*>(mount.release()));

    // For each tape we will see if the retrieve jobs are not null
    // Then we will report them as complete
    castor::tape::tapeserver::daemon::RecallReportPacker rrp(retrieveMount.get(),lc);
    rrp.startThreads();
    for(uint64_t j = 1; j<=nbArchiveFilesPerTape; ++j)
    {
      auto jobBatch = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);
      auto retrieveJob = std::unique_ptr<cta::RetrieveJob>(jobBatch.front().release());
      ASSERT_NE(nullptr, retrieveJob.get());
      rrp.reportCompletedJob(std::move(retrieveJob), lc);
    }

    rrp.setDiskDone();
    rrp.setTapeDone();
    rrp.reportDriveStatus(cta::common::dataStructures::DriveStatus::Unmounting, std::nullopt, lc);
    rrp.reportEndOfSession(lc);
    rrp.waitThread();
  }
  //Do the reporting of RetrieveJobs, will transform the Retrieve request in Archive requests
  {
    while (true) {
      auto rep = schedulerDB.getNextRepackReportBatch(lc);
      if (nullptr == rep) break;
      rep->report(lc);
    }
  }

  scheduler.waitSchedulerDbSubthreadsComplete();

  // WriteMaxDrives pre-defined value is 1
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting WriteMaxDrives to 0 means that no mount should be scheduled
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,0);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting User VO WriteMaxDrives to 1
  // Repack VO still has WriteMaxDrives as 0, so no repack mount should be scheduled
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,1);
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

  // Setting Repack VO WriteMaxDrives to 1
  // One repack mount can be scheduled
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,1);
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,"drive",lc));

}

TEST_P(SchedulerTest, retrieveArchiveAllTypesMaxDrivesVoInFlightChangeScheduleMount)
{
  //This test will emulate 3 tapeservers that will try to schedule one ArchiveForRepack, one ArchiveForUser and one Retrieve mount at the same time
  //The VO readMaxDrives and writeMaxDrives will be changed to test that it works well.
  //Also we will create two tapepools within the same VO to ensure that the readMaxDrives and writeMaxDrives are not per-tapepool
  using namespace cta;
  using namespace cta::objectstore;
  unitTests::TempDirectory tempDirectory;
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

  std::string drive1 = "drive1";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive1);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }
  std::string drive2 = "drive2";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive2);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }
  std::string drive3 = "drive3";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive3);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //This tape will contain files for triggering a Retrieve
  auto tape1 = getDefaultTape();
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape1);

  //Two tapes for ArchiveForUser and ArchiveForRepack mounts
  std::string vid2 = "VID_2";
  auto tape2 = tape1;
  tape2.vid = vid2;
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape2);

  //Create a new tapepool on the same VO
  std::string newTapepool = "new_tapepool";
  catalogue.TapePool()->createTapePool(s_adminOnAdminHost,newTapepool,s_vo,1,false,std::nullopt,"Test");

  //Create the third tape in the new tapepool
  std::string vid3 = "VID_3";
  auto tape3  = tape1;
  tape3.vid = vid3;
  tape3.tapePoolName = newTapepool;
  catalogue.Tape()->createTape(s_adminOnAdminHost,tape3);

  //Create a storage class in the catalogue
  common::dataStructures::StorageClass storageClass;
  storageClass.name = s_storageClassName;
  storageClass.nbCopies = 2;
  storageClass.comment = "Create storage class";

  catalogue.StorageClass()->modifyStorageClassNbCopies(s_adminOnAdminHost,storageClass.name,storageClass.nbCopies);

   //Create the new archive routes for the second copy
  catalogue.ArchiveRoute()->createArchiveRoute(s_adminOnAdminHost,storageClass.name,2,newTapepool,"ArchiveRoute2");

  const std::string tapeDrive = "tape_drive";
  const uint64_t nbArchiveFilesPerTape = 10;
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  //Simulate the writing of 10 files in the first tape in the catalogue
  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  {
    uint64_t archiveFileId = 1;
    std::string currentVid = s_vid;
    for(uint64_t j = 1; j <= nbArchiveFilesPerTape; ++j) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + archiveFileId);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_"<<1<<"_"<< j;
      auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
      auto & fileWritten = *fileWrittenUP;
      fileWritten.archiveFileId = archiveFileId++;
      fileWritten.diskInstance = s_diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_OWNER_UID;
      fileWritten.diskFileGid = PUBLIC_GID;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32,"1234");
      fileWritten.storageClassName = s_storageClassName;
      fileWritten.vid = currentVid;
      fileWritten.fSeq = j;
      fileWritten.blockId = j * 100;
      fileWritten.size = archiveFileSize;
      fileWritten.copyNb = 1;
      fileWritten.tapeDrive = tapeDrive;
      tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
    }
    //update the DB tape
    catalogue.TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
    tapeFilesWrittenCopy1.clear();
  }
  //Queue the Retrieve request
  scheduler.waitSchedulerDbSubthreadsComplete();
  {
    std::string diskInstance="disk_instance";
    cta::common::dataStructures::RetrieveRequest rReq;
    rReq.archiveFileID=1;
    rReq.requester.name = s_userName;
    rReq.requester.group = "someGroup";
    rReq.dstURL = "dst_url";
    scheduler.queueRetrieve(diskInstance, rReq, lc);
    scheduler.waitSchedulerDbSubthreadsComplete();
  }
  uint64_t nbArchiveRequestToQueue = 2;
  Sorter sorter(agentReference,backend,catalogue);
  for(uint64_t i = 0; i < nbArchiveRequestToQueue; ++i) {
    std::shared_ptr<cta::objectstore::ArchiveRequest> ar(new cta::objectstore::ArchiveRequest(agentReference.nextId("RepackSubRequest"),backend));
    ar->initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = i;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = archiveFileSize;
    aFile.storageClass = "sc";
    ar->setArchiveFile(aFile);
    ar->addJob(1, s_tapePoolName, agentReference.getAgentAddress(), 1, 1, 1);
    ar->addJob(2, newTapepool, agentReference.getAgentAddress(), 1, 1, 1);
    ar->setJobStatus(1, serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
    ar->setJobStatus(2, serializers::ArchiveJobStatus::AJS_ToTransferForUser);
    cta::common::dataStructures::MountPolicy mp;
    mp.name = s_mountPolicyName;
    ar->setMountPolicy(mp);
    ar->setArchiveReportURL("");
    ar->setArchiveErrorReportURL("");
    ar->setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar->setSrcURL("root://eoseos/myFile");
    ar->setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    sorter.insertArchiveRequest(ar, agentReference, lc);
    ar->insert();
  }

  sorter.flushAll(lc);

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);
  catalogue.MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);

  //Wait 2 second to be sure the minRequestAge will not prevent a mount
  ::sleep(1);

  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive1,lc));

  //No read nor write allowed on any VO
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,0);
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,0);
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_repack_vo,0);

  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive1,lc));

  //Allow one drive for write and trigger the mount
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,1);

  //Set the tape 1 to disabled state to prevent the mount in it (should be the Retrieve)
  //ArchiveForUser should have priority over ArchiveForRepack
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost,tape1.vid,cta::common::dataStructures::Tape::State::DISABLED,std::nullopt,"test");
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive1,lc));
  {
    std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,drive1,lc);

    ASSERT_NE(nullptr,tapeMount);

    tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);

    std::unique_ptr<cta::ArchiveMount> archiveForUserMount(static_cast<ArchiveMount *>(tapeMount.release()));

    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser,archiveForUserMount->getMountType());

    auto archiveForUserJobs = archiveForUserMount->getNextJobBatch(nbArchiveRequestToQueue,archiveFileSize * nbArchiveRequestToQueue,lc);

    //Pop only one file for this mount
    ASSERT_EQ(nbArchiveRequestToQueue,archiveForUserJobs.size());
  }

  //As only one drive for write is allowed, no mount should be triggered by another drive
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive2,lc));

  //Now allocate one more drive for Archival
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,1);

  //A new Archive mount should be triggered
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive2,lc));
  {
    std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,drive2,lc);

    ASSERT_NE(nullptr,tapeMount);

    tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);

    std::unique_ptr<cta::ArchiveMount> archiveForRepackMount(static_cast<ArchiveMount *>(tapeMount.release()));

    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack,archiveForRepackMount->getMountType());

    auto archiveForRepackJobs = archiveForRepackMount->getNextJobBatch(1,archiveFileSize,lc);

    //Pop only one file for this mount
    ASSERT_EQ(1,archiveForRepackJobs.size());
  }

  //As 2 drives are writing and only 2 drives are allowed on this VO, the third drive should not trigger a new mount
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive3,lc));
  //Now allocate one drive for Retrieve
  catalogue.VO()->modifyVirtualOrganizationReadMaxDrives(s_adminOnAdminHost,s_vo,1);
  //The retrieve mount should not be triggered as the tape 1 is disabled
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive3,lc));
  //Setting the state of the tape back to active
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost, tape1.vid, common::dataStructures::Tape::ACTIVE, std::nullopt,
    std::nullopt);
  //The mount should be triggered on tape 1
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive3,lc));
  //The mount should be a Retrieve mount
  {
    std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,drive3,lc);

    ASSERT_NE(nullptr,tapeMount);

    tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);

    std::unique_ptr<cta::RetrieveMount> retrieveMount(static_cast<RetrieveMount *>(tapeMount.release()));

    ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve,retrieveMount->getMountType());

    auto retrieveJobs = retrieveMount->getNextJobBatch(1,archiveFileSize,lc);

    //Pop only one file for this mount
    ASSERT_EQ(1,retrieveJobs.size());
  }
}

TEST_P(SchedulerTest, getQueuesAndMountSummariesTest)
{
  using namespace cta;
  using namespace cta::objectstore;
  using cta::common::dataStructures::JobQueueType;
  unitTests::TempDirectory tempDirectory;
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
  cta::objectstore::AgentReference agentReference("getQueuesAndMountSummariesTestAgent", dl);
  cta::objectstore::Agent agent(agentReference.getAgentAddress(), backend);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create a logical library in the catalogue
  const bool logicalLibraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  //Create two tapes
  auto tape = getDefaultTape();
  catalogue.Tape()->createTape(s_adminOnAdminHost, tape);

  std::string vid2 = s_vid + "2";
  tape.vid = vid2;
  catalogue.Tape()->createTape(s_adminOnAdminHost,tape);

  //Create a RetrieveQueue with the vid s_vid
  std::string retrieveQueueAddress;
  cta::objectstore::RootEntry re(backend);
  {
    cta::objectstore::ScopedExclusiveLock sel(re);
    re.fetch();
    retrieveQueueAddress = re.addOrGetRetrieveQueueAndCommit(s_vid,agentReference,JobQueueType::JobsToTransferForUser);
  }

  //Create a RetrieveJob and put it in the queue s_vid
  cta::objectstore::RetrieveQueue::JobToAdd retrieveJobToAdd;
  retrieveJobToAdd.copyNb = 1;
  retrieveJobToAdd.fSeq = 1;
  retrieveJobToAdd.fileSize = 1;
  retrieveJobToAdd.startTime = time(nullptr);
  retrieveJobToAdd.retrieveRequestAddress = "";

  cta::objectstore::RetrieveQueue retrieveQueue1(retrieveQueueAddress,backend);
  {
    cta::objectstore::ScopedExclusiveLock sel(retrieveQueue1);
    retrieveQueue1.fetch();
    std::list<cta::objectstore::RetrieveQueue::JobToAdd> jobsToAdd({retrieveJobToAdd});
    retrieveQueue1.addJobsAndCommit(jobsToAdd,agentReference,lc);
  }

  //Create a second retrieve queue that will hold a job for the tape vid2
  std::string retrieveQueue2Address;
  {
    cta::objectstore::ScopedExclusiveLock sel(re);
    re.fetch();
    retrieveQueue2Address = re.addOrGetRetrieveQueueAndCommit(vid2,agentReference,JobQueueType::JobsToTransferForUser);
  }
  cta::objectstore::RetrieveQueue retrieveQueue2(retrieveQueue2Address,backend);
  {
    cta::objectstore::ScopedExclusiveLock sel(retrieveQueue2);
    retrieveQueue2.fetch();
    std::list<cta::objectstore::RetrieveQueue::JobToAdd> jobsToAdd({retrieveJobToAdd});
    retrieveQueue2.addJobsAndCommit(jobsToAdd,agentReference,lc);
  }

  //Create an ArchiveForUser queue and put one file on it
  std::string archiveForUserQueueAddress;
  {
    cta::objectstore::ScopedExclusiveLock sel(re);
    re.fetch();
    archiveForUserQueueAddress = re.addOrGetArchiveQueueAndCommit(s_tapePoolName,agentReference,JobQueueType::JobsToTransferForUser);
  }

  cta::objectstore::ArchiveQueue::JobToAdd archiveJobToAdd;
  archiveJobToAdd.archiveFileId = 1;
  archiveJobToAdd.fileSize = 2;
  archiveJobToAdd.startTime = time(nullptr);

  cta::objectstore::ArchiveQueue aq(archiveForUserQueueAddress,backend);
  {
    cta::objectstore::ScopedExclusiveLock sel(aq);
    aq.fetch();
    std::list<cta::objectstore::ArchiveQueue::JobToAdd> jobsToAdd({archiveJobToAdd});
    aq.addJobsAndCommit(jobsToAdd,agentReference,lc);
  }

  // Create an ArchiveForRepack queue and put one file on it

  std::string archiveForRepackQueueAddress;
  {
    cta::objectstore::ScopedExclusiveLock sel(re);
    re.fetch();
    archiveForRepackQueueAddress = re.addOrGetArchiveQueueAndCommit(s_tapePoolName,agentReference,JobQueueType::JobsToTransferForRepack);
  }

  cta::objectstore::ArchiveQueue::JobToAdd repackArchiveJob;
  repackArchiveJob.archiveFileId = 2;
  repackArchiveJob.fileSize = 3;
  repackArchiveJob.startTime = time(nullptr);

  cta::objectstore::ArchiveQueue repackArchiveQueue(archiveForRepackQueueAddress,backend);
  {
    cta::objectstore::ScopedExclusiveLock sel(repackArchiveQueue);
    repackArchiveQueue.fetch();
    std::list<cta::objectstore::ArchiveQueue::JobToAdd> jobsToAdd({repackArchiveJob});
    repackArchiveQueue.addJobsAndCommit(jobsToAdd,agentReference,lc);
  }

  auto queuesAndMountSummaries = scheduler.getQueuesAndMountSummaries(lc);

  ASSERT_EQ(4,queuesAndMountSummaries.size());
  std::string vid = tape.vid;

  //Test the QueueAndMountSummary of the first Retrieve Queue s_vid
  auto res = std::find_if(queuesAndMountSummaries.begin(), queuesAndMountSummaries.end(), [vid](const cta::common::dataStructures::QueueAndMountSummary & qams){
    return qams.mountType == cta::common::dataStructures::MountType::Retrieve && qams.vid == vid;
  });
  ASSERT_EQ(tape.vid,res->vid);
  ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve,res->mountType);

  vid = vid2;
  //Test the QueueAndMountSummary of the first Retrieve Queue vid2
  res = std::find_if(queuesAndMountSummaries.begin(), queuesAndMountSummaries.end(), [vid](const cta::common::dataStructures::QueueAndMountSummary & qams){
    return qams.mountType == cta::common::dataStructures::MountType::Retrieve && qams.vid == vid;
  });
  ASSERT_EQ(vid,res->vid);
  ASSERT_EQ(cta::common::dataStructures::MountType::Retrieve,res->mountType);

  //Test the ArchiveForUser QueueAndMountSummary
  std::string tapePool = s_tapePoolName;
  res = std::find_if(queuesAndMountSummaries.begin(), queuesAndMountSummaries.end(), [tapePool](const cta::common::dataStructures::QueueAndMountSummary & qams){
    return qams.mountType == cta::common::dataStructures::MountType::ArchiveForUser && qams.tapePool == tapePool;
  });
  ASSERT_EQ(tapePool,res->tapePool);
  ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser,res->mountType);

  //Test the ArchiveForRepack QueueAndMountSummary
  res = std::find_if(queuesAndMountSummaries.begin(), queuesAndMountSummaries.end(), [tapePool](const cta::common::dataStructures::QueueAndMountSummary & qams){
    return qams.mountType == cta::common::dataStructures::MountType::ArchiveForRepack && qams.tapePool == tapePool;
  });
  ASSERT_EQ(tapePool, res->tapePool);
  ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack,res->mountType);
}

//This test tests what is described in the use case ticket
// high priority Archive job not scheduled when Repack is running : https://gitlab.cern.ch/cta/operations/-/issues/150
TEST_P(SchedulerTest, getNextMountWithArchiveForUserAndArchiveForRepackShouldReturnBothMountsArchiveMinRequestAge){
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
  cta::objectstore::AgentReference agentReference("agentTest", dl);
  cta::objectstore::Agent agent(agentReference.getAgentAddress(), backend);
  agent.initialize();
  agent.setTimeout_us(0);
  agent.insertAndRegisterSelf(lc);

  //Create environment for the test
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_vo,1);
  catalogue.VO()->modifyVirtualOrganizationWriteMaxDrives(s_adminOnAdminHost,s_repack_vo,1);

  std::string drive0 = "drive0";
  std::string drive1 = "drive1";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive1);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }
  std::string drive2 = "drive2";
  {
    const auto tapeDrive = getDefaultTapeDrive(drive2);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }

  //Create two tapes (ArchiveForRepack and ArchiveForUser)
  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  {
    auto tape = getDefaultTape();
    tape.vid = s_vid+"_1";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  uint64_t fileSize = 667;

  Sorter sorter(agentReference,backend,catalogue);
  for(uint64_t i = 0; i < 2; ++i) {
    std::shared_ptr<cta::objectstore::ArchiveRequest> ar(new cta::objectstore::ArchiveRequest(agentReference.nextId("RepackSubRequest"),backend));
    ar->initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = i;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = fileSize;
    aFile.storageClass = "sc";
    ar->setArchiveFile(aFile);
    ar->addJob(1, s_tapePoolName, agentReference.getAgentAddress(), 1, 1, 1);
    ar->setJobStatus(1, serializers::ArchiveJobStatus::AJS_ToTransferForRepack);
    cta::common::dataStructures::MountPolicy mp;
    //We want the archiveMinRequestAge to be taken into account and trigger the mount
    //hence set it to 0
    mp.name = s_mountPolicyName;
    ar->setMountPolicy(mp);
    ar->setArchiveReportURL("");
    ar->setArchiveErrorReportURL("");
    ar->setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar->setSrcURL("root://eoseos/myFile");
    ar->setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    sorter.insertArchiveRequest(ar, agentReference, lc);
    ar->insert();
  }

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,100);

  sorter.flushAll(lc);

  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive0,lc));

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);

  //The archiveMinRequestAge should have 1 second to trigger a mount
  ::sleep(1);

  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive0,lc));

  {
    std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,drive0,lc);

    ASSERT_NE(nullptr,tapeMount);

    tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);

    std::unique_ptr<cta::ArchiveMount> archiveForRepackMount(static_cast<ArchiveMount *>(tapeMount.release()));

    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForRepack,archiveForRepackMount->getMountType());

    auto archiveForRepackJobs = archiveForRepackMount->getNextJobBatch(1,fileSize,lc);

    //Pop only one file for this mount
    ASSERT_EQ(1,archiveForRepackJobs.size());
  }
  //Now queue ArchiveForUser files
  for(uint64_t i = 0; i < 2; ++i) {
    std::shared_ptr<cta::objectstore::ArchiveRequest> ar(new cta::objectstore::ArchiveRequest(agentReference.nextId("ArchiveRequest"),backend));
    ar->initialize();
    cta::common::dataStructures::ArchiveFile aFile;
    aFile.archiveFileID = i;
    aFile.diskFileId = "eos://diskFile";
    aFile.checksumBlob.insert(cta::checksum::NONE, "");
    aFile.creationTime = 0;
    aFile.reconciliationTime = 0;
    aFile.diskFileInfo = cta::common::dataStructures::DiskFileInfo();
    aFile.diskInstance = "eoseos";
    aFile.fileSize = fileSize;
    aFile.storageClass = "sc";
    ar->setArchiveFile(aFile);
    ar->addJob(1, s_tapePoolName, agentReference.getAgentAddress(), 1, 1, 1);
    ar->setJobStatus(1, serializers::ArchiveJobStatus::AJS_ToTransferForUser);
    cta::common::dataStructures::MountPolicy mp;
    //We want the archiveMinRequestAge to be taken into account and trigger the mount
    //hence set it to 0
    mp.name = s_mountPolicyName;
    ar->setMountPolicy(mp);
    ar->setArchiveReportURL("");
    ar->setArchiveErrorReportURL("");
    ar->setRequester(cta::common::dataStructures::RequesterIdentity("user0", "group0"));
    ar->setSrcURL("root://eoseos/myFile");
    ar->setEntryLog(cta::common::dataStructures::EntryLog("user0", "host0", time(nullptr)));
    sorter.insertArchiveRequest(ar, agentReference, lc);
    ar->insert();
  }

  sorter.flushAll(lc);

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,100);
  //mount should not be triggered
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive0,lc));

  catalogue.MountPolicy()->modifyMountPolicyArchiveMinRequestAge(s_adminOnAdminHost,s_mountPolicyName,0);

  //Sleeping one seconds to trigger a mount
  ::sleep(1);

  //The next mount should be an ArchiveForUser mount as there is already a mount ongoing with an ArchiveForRepack
  ASSERT_TRUE(scheduler.getNextMountDryRun(s_libraryName,drive1,lc));
  {
    std::unique_ptr<cta::TapeMount> tapeMount = scheduler.getNextMount(s_libraryName,drive1,lc);

    ASSERT_NE(nullptr,tapeMount);

    tapeMount->setDriveStatus(cta::common::dataStructures::DriveStatus::Starting);

    std::unique_ptr<cta::ArchiveMount> archiveForUserMount(static_cast<ArchiveMount *>(tapeMount.release()));

    ASSERT_EQ(cta::common::dataStructures::MountType::ArchiveForUser,archiveForUserMount->getMountType());

    auto archiveForUserJobs = archiveForUserMount->getNextJobBatch(1,fileSize,lc);

    //Pop only one file for this mount
    ASSERT_EQ(1,archiveForUserJobs.size());
  }

  //Now let's create another tape, and try to schedule another mount with another drive
  //No ArchiveMount should be triggered
  {
    auto tape = getDefaultTape();
    tape.vid = s_vid+"_2";
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }
  ASSERT_FALSE(scheduler.getNextMountDryRun(s_libraryName,drive2,lc));
}

// Next two tests were added after the Issue 470, https://gitlab.cern.ch/cta/CTA/-/issues/470 
TEST_P(SchedulerTest, testCleaningUpKeepingTapePoolName) {
  using namespace cta;

  setupDefaultCatalogue();

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  {
    // Drive name to fail when it's in CleaningUp state
    const std::string driveName = "drive0";
    auto tapeDrive = catalogue.DriveState()->getTapeDrive(driveName);
    // Insert tape pool name to the drive
    tapeDrive.value().currentTapePool = s_tapePoolName;
    tapeDrive.value().driveStatus = common::dataStructures::DriveStatus::CleaningUp;
    catalogue.DriveState()->updateTapeDriveStatus(tapeDrive.value());
    // And simulate the drive had a uncaught exception in CleaningUp state, and it didn't go to Down state
    TapeDrivesCatalogueState tapeDriveState(catalogue);
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    tapeDriveState.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount,
      cta::common::dataStructures::DriveStatus::CleaningUp, time(nullptr), lc);
  }

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.Tape()->tapeLabelled(s_vid, driveName);

  ASSERT_NO_THROW(scheduler.getNextMount(s_libraryName, driveName, lc));
}

// Issue 470, https://gitlab.cern.ch/cta/CTA/-/issues/470 
TEST_P(SchedulerTest, testCleaningUpWithoutTapePoolName) {
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

  {
    // Simulate the drive had a uncaught exception in CleaningUp state,
    // and it didn't go to Down state with empty tape pool name
    TapeDrivesCatalogueState tapeDriveState(catalogue);
    cta::common::dataStructures::DriveInfo driveInfo = { "drive0", "myHost", s_libraryName };
    tapeDriveState.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount,
      cta::common::dataStructures::DriveStatus::CleaningUp, time(nullptr), lc);
  }

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }
  const std::string driveName = "tape_drive";
  catalogue.Tape()->tapeLabelled(s_vid, driveName);

  // Now it doesn't throw an exception, ISSUE 494 Workaround for scheduler crashing 
  ASSERT_NO_THROW(scheduler.getNextMount(s_libraryName, driveName, lc));
}

// Next two tests were added after the Issue 470, https://gitlab.cern.ch/cta/CTA/-/issues/470 
TEST_P(SchedulerTest, testShutdownKeepingTapePoolName) {
  using namespace cta;

  setupDefaultCatalogue();

  auto &catalogue = getCatalogue();
  auto &scheduler = getScheduler();

#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  {
    // Drive name to fail when it's in Shutdown state
    const std::string driveName = "drive0";
    auto tapeDrive = catalogue.DriveState()->getTapeDrive(driveName);
    // Insert tape pool name to the drive
    tapeDrive.value().currentTapePool = s_tapePoolName;
    tapeDrive.value().driveStatus = common::dataStructures::DriveStatus::Shutdown;
    catalogue.DriveState()->updateTapeDriveStatus(tapeDrive.value());
    // And simulate the drive had a uncaught exception in Shutdown state, and it didn't go to Down state
    TapeDrivesCatalogueState tapeDriveState(catalogue);
    cta::common::dataStructures::DriveInfo driveInfo = { driveName, "myHost", s_libraryName };
    tapeDriveState.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount,
      cta::common::dataStructures::DriveStatus::Shutdown, time(nullptr), lc);
  }

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, physicalLibraryName, libraryComment);

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  const std::string driveName = "tape_drive";
  catalogue.Tape()->tapeLabelled(s_vid, driveName);

  ASSERT_NO_THROW(scheduler.getNextMount(s_libraryName, driveName, lc));
}

// This checks valid tape state changes
TEST_P(SchedulerTestTriggerTapeStateChangeBehaviour, triggerTapeStateChangeValidScenarios){
//Queue 2 archive requests in two different logical libraries
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  auto &catalogue = getCatalogue();
  auto &schedulerDB = getSchedulerDB();

  setupDefaultCatalogue();
#ifdef STDOUT_LOGGING
  log::StdoutLogger dl("dummy", "unitTest");
#else
  log::DummyLogger dl("", "");
#endif
  log::LogContext lc(dl);

  if (!GetParam().m_triggerTapeStateChangeBehaviour.has_value()) {
    throw exception::Exception("Test needs 'TriggerTapeStateChangeBehaviour' parameters");
  }

  auto triggerTapeStateChangeBehaviour = GetParam().m_triggerTapeStateChangeBehaviour.value();

  // Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, physicalLibraryName, libraryComment);

  auto tape = getDefaultTape();
  {
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Setup initial conditions
  schedulerDB.setRetrieveQueueCleanupFlag(tape.vid, false, lc);
  catalogue.Tape()->modifyTapeState(s_adminOnAdminHost, tape.vid,triggerTapeStateChangeBehaviour.fromState,std::nullopt,"Test");

  // Trigger change
  if (triggerTapeStateChangeBehaviour.changeRaisedException) {
    ASSERT_THROW(scheduler.triggerTapeStateChange(s_adminOnAdminHost, tape.vid, triggerTapeStateChangeBehaviour.toState, "Test", lc), exception::UserError);
  } else {
    ASSERT_NO_THROW(scheduler.triggerTapeStateChange(s_adminOnAdminHost, tape.vid, triggerTapeStateChangeBehaviour.toState, "Test", lc));
  }

  // Observe results
  ASSERT_EQ(catalogue.Tape()->getTapesByVid(tape.vid).at(tape.vid).state, triggerTapeStateChangeBehaviour.observedState);
  ASSERT_EQ(schedulerDB.getRetrieveQueuesCleanupInfo(lc).front().doCleanup, triggerTapeStateChangeBehaviour.cleanupFlagActivated);
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

using Tape = cta::common::dataStructures::Tape;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS, SchedulerTestTriggerTapeStateChangeBehaviour,
                        ::testing::Values(
                                /* { fromState, toState, observedState, changeRaisedException, cleanupFlagActivated } */
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::ACTIVE,             Tape::ACTIVE,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::DISABLED,           Tape::DISABLED,           false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::REPACKING,          Tape::REPACKING_PENDING,  false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::REPACKING_PENDING,  Tape::ACTIVE,             true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::REPACKING_DISABLED, Tape::ACTIVE,             true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::BROKEN_PENDING,     Tape::ACTIVE,             true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::ACTIVE,             Tape::EXPORTED_PENDING,   Tape::ACTIVE,             true,  false}),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::ACTIVE,             Tape::ACTIVE,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::DISABLED,           Tape::DISABLED,           false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::REPACKING,          Tape::REPACKING_PENDING,  false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::REPACKING_DISABLED, Tape::DISABLED,           true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::DISABLED,           Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true }),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::ACTIVE,             Tape::ACTIVE,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::DISABLED,           Tape::DISABLED,           false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::REPACKING,          Tape::REPACKING,          false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::REPACKING_DISABLED, Tape::REPACKING_DISABLED, false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING,          Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true }),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::ACTIVE,             Tape::REPACKING_DISABLED, true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::DISABLED,           Tape::REPACKING_DISABLED, true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::REPACKING,          Tape::REPACKING,          false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::REPACKING_DISABLED, Tape::REPACKING_DISABLED, false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_DISABLED, Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true }),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::ACTIVE,             Tape::ACTIVE,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::DISABLED,           Tape::DISABLED,           false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::REPACKING,          Tape::REPACKING_PENDING,  false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::REPACKING_DISABLED, Tape::BROKEN,             true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::BROKEN,             Tape::BROKEN,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN,             Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true }),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::ACTIVE,             Tape::ACTIVE,             false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::DISABLED,           Tape::DISABLED,           false, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::REPACKING,          Tape::REPACKING_PENDING,  false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::REPACKING_DISABLED, Tape::EXPORTED,           true,  false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true }),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED,           Tape::EXPORTED,           Tape::EXPORTED,           false, false}),

                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_PENDING,  Tape::ACTIVE,             Tape::REPACKING_PENDING,  true, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN_PENDING,     Tape::ACTIVE,             Tape::BROKEN_PENDING,     true, false}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED_PENDING,   Tape::ACTIVE,             Tape::EXPORTED_PENDING,   true, false}),

                                // The 'cleanup' flag should be reactivated when the same PENDING state is re-triggered
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::REPACKING_PENDING,  Tape::REPACKING,          Tape::REPACKING_PENDING,  false, true}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::BROKEN_PENDING,     Tape::BROKEN,             Tape::BROKEN_PENDING,     false, true}),
                                SchedulerTestParam(OStoreDBFactoryVFS, {Tape::EXPORTED_PENDING,   Tape::EXPORTED,           Tape::EXPORTED_PENDING,   false, true})
                        ));

#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, SchedulerTest,
  ::testing::Values(SchedulerTestParam(OStoreDBFactoryRados)));
#endif
} // namespace unitTests

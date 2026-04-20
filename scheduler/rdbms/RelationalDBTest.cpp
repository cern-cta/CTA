/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>

namespace unitTests {

const uint32_t DISK_FILE_OWNER_UID = 9751;
const uint32_t DISK_FILE_GID = 9752;
const std::string s_storageClassName = "TestStorageClass";

cta::common::dataStructures::MountPolicy makeMountPolicy(time_t t) {
  cta::common::dataStructures::MountPolicy mountPolicy;
  mountPolicy.name = "mountPolicy";
  mountPolicy.archivePriority = 1;
  mountPolicy.archiveMinRequestAge = 0;
  mountPolicy.retrievePriority = 1;
  mountPolicy.retrieveMinRequestAge = 0;
  mountPolicy.creationLog = {"u", "h", t};
  mountPolicy.lastModificationLog = {"u", "h", t};
  mountPolicy.comment = "comment";
  return mountPolicy;
}

std::unique_ptr<cta::log::Logger> makeLogger() {
#ifdef STDOUT_LOGGING
  return std::make_unique<cta::log::StdoutLogger>("dummy", "unitTest");
#else
  return std::make_unique<cta::log::DummyLogger>("dummy", "unitTest");
#endif
}

void queueArchiveJob(cta::SchedulerDatabase& db,
                     cta::log::LogContext& lc,
                     uint64_t fileId,
                     const std::string& tapePool,
                     const std::string& suffix,
                     time_t creationTime) {
  cta::common::dataStructures::ArchiveRequest ar;
  cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

  afqc.copyToPoolMap.insert({1, tapePool});
  afqc.fileId = fileId;
  afqc.mountPolicy = makeMountPolicy(creationTime);

  ar.archiveReportURL = "test://archive-report-url";
  ar.archiveErrorReportURL = "test://error-report-url";
  ar.creationLog = {"user", "host", creationTime};
  ar.diskFileID = "diskFile-" + suffix;
  ar.diskFileInfo.path = "/path/" + suffix;
  ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  ar.diskFileInfo.gid = DISK_FILE_GID;
  ar.fileSize = 1000 + fileId;
  ar.requester = {"user", "group"};
  ar.srcURL = "root:/path/" + suffix;
  ar.storageClass = "storageClass";

  db.queueArchive("eosInstance", ar, afqc, lc);
}

void queueRetrieveJob(cta::SchedulerDatabase& db,
                      cta::log::LogContext& lc,
                      uint64_t archiveFileId,
                      const std::string& vid,
                      const std::string& suffix,
                      time_t creationTime) {
  cta::common::dataStructures::RetrieveRequest rr;
  cta::common::dataStructures::RetrieveFileQueueCriteria rfqc;

  rfqc.mountPolicy = makeMountPolicy(creationTime);

  rfqc.archiveFile.archiveFileID = archiveFileId;
  rfqc.archiveFile.fileSize = 4000 + archiveFileId;
  rfqc.archiveFile.storageClass = s_storageClassName;
  rfqc.archiveFile.diskInstance = "diskInstance";
  rfqc.archiveFile.diskFileId = "diskFile-" + suffix;
  rfqc.archiveFile.diskFileInfo.path = "/retrieve/" + suffix;
  rfqc.archiveFile.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
  rfqc.archiveFile.diskFileInfo.gid = DISK_FILE_GID;

  rfqc.archiveFile.tapeFiles.emplace_back();
  rfqc.archiveFile.tapeFiles.back().fSeq = archiveFileId;
  rfqc.archiveFile.tapeFiles.back().vid = vid;
  rfqc.archiveFile.tapeFiles.back().copyNb = 1;
  rfqc.archiveFile.tapeFiles.back().fileSize = 4000 + archiveFileId;

  rr.archiveFileID = archiveFileId;
  rr.creationLog = {"user", "host", creationTime};
  rr.lifecycleTimings.creation_time = creationTime;
  rr.diskFileInfo.path = "/retrieve/" + suffix;
  rr.requester = {"user", "group"};
  rr.dstURL = "root://disk/" + suffix;
  rr.errorReportURL = "test://retrieve-error-report-url";

  db.queueRetrieve(rr, rfqc, "ds-A", lc);
}

/**
 * This structure is used to parameterize RelationalDB database tests.
 */
struct RelationalDBTestParams {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit RelationalDBTestParams(cta::SchedulerDatabaseFactory* dbFactory) : dbFactory(*dbFactory) {}
};  // struct RelationalDBTestParams

/**
 * The RelationalDB database test is a parameterized test.  It takes an
 * RelationalDB database factory as a parameter.
 */
class RelationalDBTest : public ::testing::TestWithParam<RelationalDBTestParams> {
public:
  RelationalDBTest() noexcept {}

  class FailedToGetDatabase : public std::exception {
  public:
    const char* what() const noexcept { return "Failed to get scheduler database"; }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto& factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the PostgresSched DB from the factory.
    auto psdb = std::move(factory.create(m_catalogue));
    // Make sure the type of the SchedulerDatabase is correct (it should be an RelationalDBTestWrapper).
    dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::RelationalDBTestWrapper& getDb() {
    cta::RelationalDBTestWrapper* const ptr = m_db.get();
    if (nullptr == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  static const std::string s_systemHost;
  static const std::string s_adminHost;
  static const std::string s_userHost;

  static const std::string s_system;
  static const std::string s_admin;
  static const std::string s_user;

  static const cta::common::dataStructures::SecurityIdentity s_systemOnSystemHost;

  static const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_adminOnUserHost;

  static const cta::common::dataStructures::SecurityIdentity s_userOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_userOnUserHost;

private:
  // Prevent copying
  RelationalDBTest(const RelationalDBTest&) = delete;

  // Prevent assignment
  RelationalDBTest& operator=(const RelationalDBTest&) = delete;

  std::unique_ptr<cta::RelationalDBTestWrapper> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};  // class RelationalDBTest

TEST_P(RelationalDBTest, queueAndGetArchiveJobs) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  // Insert jobs into two different tape pools with one matching job and one non-matching job to verify that filtering works
  queueArchiveJob(db, lc, 111, "tapePoolA", "A", 1000);
  queueArchiveJob(db, lc, 222, "tapePoolB", "B", 2000);

  // Filter by tapePoolA
  // This should return only the job in tapePoolA
  const auto jobs = db.getArchiveJobs(std::string("tapePoolA"));
  ASSERT_EQ(1u, jobs.size());

  // Verify returned job is really the tapePoolA and its fields are filled correctly
  const auto& job = jobs.front();
  ASSERT_EQ("tapePoolA", job.tapePool);
  ASSERT_EQ(111u, job.archiveFileID);
  ASSERT_EQ(1u, job.copyNumber);
  ASSERT_EQ(1000u, job.request.creationLog.time);
  ASSERT_EQ("diskFile-A", job.request.diskFileID);
  ASSERT_EQ("/path/A", job.request.diskFileInfo.path);
  ASSERT_EQ(DISK_FILE_OWNER_UID, job.request.diskFileInfo.owner_uid);
  ASSERT_EQ(DISK_FILE_GID, job.request.diskFileInfo.gid);
  ASSERT_EQ(1111u, job.request.fileSize);
  ASSERT_EQ("storageClass", job.request.storageClass);
  ASSERT_EQ("root:/path/A", job.request.srcURL);
  ASSERT_EQ("user", job.request.requester.name);
  ASSERT_EQ("group", job.request.requester.group);
}

TEST_P(RelationalDBTest, queueAndGetRetrieveJobs) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  // Insert jobs on two different VIDs
  // Queue one job on vidA and one on vidB to verify filtering works
  queueRetrieveJob(db, lc, 333, "vidA", "A", 3000);
  queueRetrieveJob(db, lc, 444, "vidB", "B", 4000);

  // Filter by vidA
  // This should return only the retrieve job on vidA
  const auto jobs = db.getPendingRetrieveJobs(std::string("vidA"));
  ASSERT_EQ(1u, jobs.size());

  // Verify the returned job is the vidA job and its fields are filled correctly
  const auto& job = jobs.front();
  ASSERT_EQ(333u, job.request.archiveFileID);
  ASSERT_EQ(3000u, job.request.creationLog.time);
  ASSERT_EQ("root://disk/A?oss.asize=4333", job.request.dstURL);
  ASSERT_EQ("/retrieve/A", job.request.diskFileInfo.path);
  ASSERT_EQ(DISK_FILE_OWNER_UID, job.request.diskFileInfo.owner_uid);
  ASSERT_EQ(DISK_FILE_GID, job.request.diskFileInfo.gid);
  ASSERT_EQ(4333u, job.fileSize);
  ASSERT_EQ("diskInstance", job.diskInstance);
  ASSERT_EQ(s_storageClassName, job.storageClass);
  ASSERT_EQ("diskFile-A", job.diskFileId);
  ASSERT_EQ(1u, job.tapeCopies.size());
  ASSERT_TRUE(job.tapeCopies.find("vidA") != job.tapeCopies.end());
  ASSERT_EQ(1u, job.tapeCopies.at("vidA").first);
}

TEST_P(RelationalDBTest, queueArchiveAndCheckTapePool) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  queueArchiveJob(db, lc, 112, "tapePoolA", "A2", 1001);
  queueArchiveJob(db, lc, 221, "tapePoolB", "B1", 2000);

  // Query all archive jobs
  // This should return jobs grouped by tape pool
  const auto jobs = db.getArchiveJobs();

  ASSERT_EQ(2u, jobs.size());
  ASSERT_EQ(2u, jobs.at("tapePoolA").size());
  ASSERT_EQ(1u, jobs.at("tapePoolB").size());
}

TEST_P(RelationalDBTest, queueRetrieveAndCheckVid) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  queueRetrieveJob(db, lc, 333, "vidA", "A1", 3000);
  queueRetrieveJob(db, lc, 334, "vidA", "A2", 3001);
  queueRetrieveJob(db, lc, 444, "vidB", "B1", 4000);

  // Query all retrieve jobs
  // This should return jobs grouped by VID
  const auto jobs = db.getPendingRetrieveJobs();
  ASSERT_EQ(2u, jobs.size());
  ASSERT_EQ(2u, jobs.at("vidA").size());
  ASSERT_EQ(1u, jobs.at("vidB").size());
}

TEST_P(RelationalDBTest, queueRepack) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  auto mountPolicy = makeMountPolicy(1000);

  cta::SchedulerDatabase::QueueRepackRequest repackRequest("V12345",
                                                           "/repack/buffer",
                                                           cta::common::dataStructures::RepackInfo::Type::MoveOnly,
                                                           mountPolicy,
                                                           false,
                                                           10);

  repackRequest.m_creationLog = {"user", "host", 1000};

  db.queueRepack(repackRequest, lc);

  ASSERT_TRUE(db.repackExists());

  const auto repackInfo = db.getRepackInfo("V12345");

  ASSERT_EQ("V12345", repackInfo.vid);
  ASSERT_EQ("/repack/buffer", repackInfo.repackBufferBaseURL);
  ASSERT_EQ(cta::common::dataStructures::RepackInfo::Type::MoveOnly, repackInfo.type);
  ASSERT_EQ("mountPolicy", repackInfo.mountPolicy);
  ASSERT_EQ(1000u, repackInfo.creationLog.time);
  ASSERT_EQ(10u, repackInfo.maxFilesToSelect);
  ASSERT_FALSE(repackInfo.noRecall);
}

TEST_P(RelationalDBTest, queueRepackInitial) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();

  auto mountPolicy = makeMountPolicy(1000);

  // Repack request
  cta::SchedulerDatabase::QueueRepackRequest repackRequest("V54321",
                                                           "/repack/buffer2",
                                                           cta::common::dataStructures::RepackInfo::Type::AddCopiesOnly,
                                                           mountPolicy,
                                                           true,
                                                           5);

  repackRequest.m_creationLog = {"user", "host", 2000};

  db.queueRepack(repackRequest, lc);

  // Retrieve the repack info
  const auto repackInfo = db.getRepackInfo("V54321");

  // Verify that a newly queued repack has the correct initial state in the db

  // newly queued repack should be pending
  ASSERT_EQ(cta::common::dataStructures::RepackInfo::Status::Pending, repackInfo.status);
  ASSERT_EQ(cta::common::dataStructures::RepackInfo::Type::AddCopiesOnly, repackInfo.type);
  ASSERT_EQ("mountPolicy", repackInfo.mountPolicy);
  ASSERT_EQ(2000u, repackInfo.creationLog.time);
  ASSERT_EQ(5u, repackInfo.maxFilesToSelect);
  ASSERT_TRUE(repackInfo.noRecall);
  ASSERT_FALSE(repackInfo.isExpandStarted);
  ASSERT_FALSE(repackInfo.isExpandFinished);
  ASSERT_TRUE(repackInfo.destinationInfos.empty());
  // all counters should be zero at initial state
  ASSERT_EQ(0u, repackInfo.retrievedFiles);
  ASSERT_EQ(0u, repackInfo.retrievedBytes);
  ASSERT_EQ(0u, repackInfo.archivedFiles);
  ASSERT_EQ(0u, repackInfo.archivedBytes);
  ASSERT_EQ(0u, repackInfo.failedFilesToRetrieve);
  ASSERT_EQ(0u, repackInfo.failedBytesToRetrieve);
  ASSERT_EQ(0u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(0u, repackInfo.failedBytesToArchive);
}

static cta::RelationalDBTestFactory RelationalDBTestFactoryStatic;
INSTANTIATE_TEST_CASE_P(RelationalDBTest,
                        RelationalDBTest,
                        ::testing::Values(RelationalDBTestParams(&RelationalDBTestFactoryStatic)));

}  // namespace unitTests

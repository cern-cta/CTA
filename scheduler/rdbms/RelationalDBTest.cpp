/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/TapeForWriting.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/RetrieveMount.hpp"
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
  rr.diskFileInfo.path = "/retrieve/" + suffix;
  rr.requester = {"user", "group"};
  rr.dstURL = "root://disk/" + suffix;
  rr.errorReportURL = "test://retrieve-error-report-url";

  db.queueRetrieve(rr, rfqc, "ds-A", lc);
}

void queueMultiCopyArchiveJob(cta::SchedulerDatabase& db,
                              cta::log::LogContext& lc,
                              uint64_t fileId,
                              const std::string& tapePool,
                              const std::string& suffix,
                              time_t creationTime) {
  cta::common::dataStructures::ArchiveRequest ar;
  cta::common::dataStructures::ArchiveFileQueueCriteriaAndFileId afqc;

  // Two copies to the same tape pool so both land in the same pending queue
  afqc.copyToPoolMap.insert({1, tapePool});
  afqc.copyToPoolMap.insert({2, tapePool});
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
    auto psdb = factory.create(m_catalogue);
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

TEST_P(RelationalDBTest, retrieveCreationTimeStaysUnchangedAfterPopAndRequeue) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  constexpr time_t creationTime = 3000;
  queueRetrieveJob(db, lc, 333, "vidA", "A", creationTime);
  db.waitSubthreadsComplete();

  auto checkActiveCreationTime = [&]() {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("SELECT CREATION_TIME FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();

    ASSERT_TRUE(rset.next());
    ASSERT_EQ(static_cast<uint64_t>(creationTime), rset.columnUint64("CREATION_TIME"));
    ASSERT_FALSE(rset.next());
  };

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "library", "host");

  auto jobs = mount->getNextJobBatch(1, 10000, lc);
  ASSERT_EQ(1u, jobs.size());
  ASSERT_EQ(creationTime, jobs.front()->retrieveRequest.creationLog.time);
  ASSERT_NO_FATAL_FAILURE(checkActiveCreationTime());

  mount->requeueJobBatch(jobs, lc);

  jobs = mount->getNextJobBatch(1, 10000, lc);
  ASSERT_EQ(1u, jobs.size());
  ASSERT_EQ(creationTime, jobs.front()->retrieveRequest.creationLog.time);
  ASSERT_NO_FATAL_FAILURE(checkActiveCreationTime());
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

TEST_P(RelationalDBTest, queueRepackWithStorageClass) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  cta::SchedulerDatabase& db = getDb();
  auto mountPolicy = makeMountPolicy(1000);

  const std::string storageClass = "TestStorageClass";

  cta::SchedulerDatabase::QueueRepackRequest repackRequest("V99999",
                                                           "/repack/buffer",
                                                           cta::common::dataStructures::RepackInfo::Type::MoveOnly,
                                                           mountPolicy,
                                                           false,
                                                           10,
                                                           storageClass);

  repackRequest.m_creationLog = {"user", "host", 1000};

  db.queueRepack(repackRequest, lc);

  const auto repackInfo = db.getRepackInfo("V99999");

  ASSERT_EQ(storageClass, repackInfo.storageClass);
}

TEST_P(RelationalDBTest, deleteExpiredDiskSystemSleepEntries) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  RelationalDB& relationalDb = getDb().getRelationalDB();
  const uint64_t now = static_cast<uint64_t>(cta::utils::getCurrentEpochTime());

  // Insert the fixture rows directly: insertOrUpdateDiskSleepEntry() needs a Transaction taken
  // from the connection pool, which is not exposed outside RelationalDB.
  auto conn = relationalDb.getConn();
  auto insertSleepEntry = [&conn](const std::string& diskSystemName, uint64_t sleepTime, uint64_t lastUpdateTime) {
    auto stmt = conn.createStmt(R"SQL(
      INSERT INTO DISK_SYSTEM_SLEEP_TRACKING (DISK_SYSTEM_NAME, SLEEP_TIME, LAST_UPDATE_TIME)
      VALUES (:DISK_SYSTEM_NAME, :SLEEP_TIME, :LAST_UPDATE_TIME)
    )SQL");
    stmt.bindString(":DISK_SYSTEM_NAME", diskSystemName);
    stmt.bindUint64(":SLEEP_TIME", sleepTime);
    stmt.bindUint64(":LAST_UPDATE_TIME", lastUpdateTime);
    stmt.executeNonQuery();
  };

  insertSleepEntry("ds-expired", 10, now - 3600);  // sleep window elapsed an hour ago
  insertSleepEntry("ds-active", 3600, now);        // still sleeping

  // Only the expired entry should be removed
  ASSERT_EQ(1u, relationalDb.deleteExpiredDiskSystemSleepEntries(lc));

  const auto activeEntries = relationalDb.getDiskSystemSleepActiveEntries(conn);
  ASSERT_EQ(1u, activeEntries.size());
  ASSERT_TRUE(activeEntries.find("ds-active") != activeEntries.end());
  ASSERT_TRUE(activeEntries.find("ds-expired") == activeEntries.end());

  // A second run finds nothing left to remove
  ASSERT_EQ(0u, relationalDb.deleteExpiredDiskSystemSleepEntries(lc));
}

TEST_P(RelationalDBTest, archiveFailTransferBatchRequeue) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  queueArchiveJob(db, lc, 222, "tapePoolA", "A2", 2000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // First failure: totalRetries becomes 1, maxTotalRetries=2 → requeue path
  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("test failure");
    batch.push_back(job.get());
  }

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM ARCHIVE_PENDING_QUEUE"
      " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RETRIES"));
  }
}

TEST_P(RelationalDBTest, archiveFailTransferBatchExceedRetries) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  queueArchiveJob(db, lc, 222, "tapePoolA", "A2", 2000);
  db.waitSubthreadsComplete();

  // Force maxTotalRetries=1 so first failure immediately exceeds the limit
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // First failure: totalRetries becomes 1 = maxTotalRetries=1 → exceed path
  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("test failure");
    batch.push_back(job.get());
  }

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
      " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

TEST_P(RelationalDBTest, retrieveFailTransferBatchRequeue) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueRetrieveJob(db, lc, 333, "vidA", "A1", 3000);
  queueRetrieveJob(db, lc, 444, "vidA", "A2", 4000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // First failure: totalRetries becomes 1 < maxTotalRetries=6 → requeue path
  std::vector<SchedulerDatabase::RetrieveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("test failure");
    batch.push_back(job.get());
  }

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM RETRIEVE_PENDING_QUEUE"
      " WHERE STATUS = 'RJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RETRIES"));
  }
}

TEST_P(RelationalDBTest, retrieveFailTransferBatchExceedRetries) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueRetrieveJob(db, lc, 333, "vidA", "A1", 3000);
  queueRetrieveJob(db, lc, 444, "vidA", "A2", 4000);
  db.waitSubthreadsComplete();

  // Force maxTotalRetries=1 so first failure immediately exceeds the limit
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // First failure: totalRetries becomes 1 = maxTotalRetries=1 → exceed path
  std::vector<SchedulerDatabase::RetrieveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("test failure");
    batch.push_back(job.get());
  }

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE"
      " WHERE STATUS = 'RJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_PENDING_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

TEST_P(RelationalDBTest, archiveFailTransferBatchRequeue_multiCopy) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // One 2-copy request: both copies land in tapePoolA
  queueMultiCopyArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Fail only the first copy (totalRetries=1 < maxTotalRetries=2 → requeue path)
  // The second copy stays untouched in ARCHIVE_ACTIVE_QUEUE
  auto& failedJob = jobs.front();
  failedJob->prepareFailTransfer("test failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {failedJob.get()};

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Failed copy is requeued; the other copy stays in the active queue untouched
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
      " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM ARCHIVE_PENDING_QUEUE"
      " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RETRIES"));
  }
}

TEST_P(RelationalDBTest, archiveFailTransferBatchExceedRetries_multiCopy) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // One 2-copy request: both copies land in tapePoolA
  queueMultiCopyArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  // Force maxTotalRetries=1 so the first failure immediately exceeds the limit
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Fail only the first copy (totalRetries=1 = maxTotalRetries=1 → exceed path)
  // Because req_job_count=2, failTransferBatchExceedRetries matches by ARCHIVE_REQUEST_ID
  // and marks BOTH copies as AJS_ToReportToUserForFailure
  auto& failedJob = jobs.front();
  failedJob->prepareFailTransfer("test failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {failedJob.get()};

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Both copies must be flagged for failure — the untouched copy is collaterally failed via
  // the ARCHIVE_REQUEST_ID match in failTransferBatchExceedRetries
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
      " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// When within-mount retries are exhausted but total retries are not, the job must
// be requeued with MOUNT_ID = NULL so it can be picked up by a different mount.
TEST_P(RelationalDBTest, archiveFailTransferBatchRequeue_withinMountExhausted) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  // MAX_RETRIES_WITHIN_MOUNT=1 means the very first failure exhausts within-mount budget.
  // MAX_TOTAL_RETRIES stays at 2, so the job is still eligible for a retry on a new mount.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "UPDATE ARCHIVE_PENDING_QUEUE SET MAX_RETRIES_WITHIN_MOUNT = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("within-mount exhausted");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Job requeued for a different mount: MOUNT_ID must be NULL
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
      " WHERE STATUS = 'AJS_ToTransferForUser' AND MOUNT_ID IS NULL");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

TEST_P(RelationalDBTest, retrieveFailTransferBatchRequeue_withinMountExhausted) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueRetrieveJob(db, lc, 333, "vidA", "A1", 3000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "UPDATE RETRIEVE_PENDING_QUEUE SET MAX_RETRIES_WITHIN_MOUNT = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("within-mount exhausted");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM RETRIEVE_PENDING_QUEUE"
      " WHERE STATUS = 'RJS_ToTransferForUser' AND MOUNT_ID IS NULL");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// A single failTransferBatch call may contain a mix of jobs that exceed their total
// retry budget (→ ToReportToUserForFailure) and jobs that are merely requeued.
// Verify the SQL correctly routes them into the two paths within one transaction.
TEST_P(RelationalDBTest, archiveFailTransferBatchMixed) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  queueArchiveJob(db, lc, 222, "tapePoolA", "A2", 2000);
  queueArchiveJob(db, lc, 333, "tapePoolA", "A3", 3000);
  db.waitSubthreadsComplete();

  // Force job A1 to have maxTotalRetries=1 so it goes to the exceed path;
  // A2 and A3 keep the default maxTotalRetries=2 and go to the requeue path.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1"
      " WHERE DISK_FILE_ID = 'diskFile-A1'");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(3u, jobs.size());

  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("test failure");
    batch.push_back(job.get());
  }

  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // A1 exceeded total retries → stays in active queue flagged for failure report
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
      " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // A2, A3 are requeued for another attempt
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
      " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
}

// The failure reason passed to prepareFailTransfer must be persisted to the
// FAILURE_LOG column so operators can diagnose why a job was retried.
TEST_P(RelationalDBTest, archiveFailTransferBatch_persistsFailureLog) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  const std::string failureReason = "distinctive-failure-sentinel-42";
  jobs.front()->prepareFailTransfer(failureReason);
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT FAILURE_LOG FROM ARCHIVE_PENDING_QUEUE WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    const auto log = rset.columnString("FAILURE_LOG");
    ASSERT_NE(std::string::npos, log.find(failureReason));
  }
}

// ===== failReportBatch tests (pgsched only) =====

// Baseline: prepareFailReport increments totalReportRetries and writes to
// REPORT_FAILURE_LOG, but does NOT touch TOTAL_RETRIES (transfer counters).
// Tests the requeue path (totalReportRetries=1 < maxReportRetries=2).
TEST_P(RelationalDBTest, archiveFailReportBatchRequeue) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  // prepareFailReport increments in-memory totalReportRetries (1 < default maxReportRetries=2)
  // so failReportBatch takes the requeue (IS_REPORTING reset) path.
  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  jobs.front()->prepareFailReport("report failure reason");
  batch.push_back(jobs.front().get());
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Job must remain in ARCHIVE_ACTIVE_QUEUE with IS_REPORTING=FALSE, TOTAL_REPORT_RETRIES=1.
  {
    auto stmt = conn.createStmt(
      "SELECT TOTAL_REPORT_RETRIES, IS_REPORTING, TOTAL_RETRIES"
      " FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_REPORT_RETRIES"));
    ASSERT_FALSE(rset.columnBool("IS_REPORTING"));
    // prepareFailReport must NOT have touched TOTAL_RETRIES (transfer counter).
    ASSERT_EQ(0u, rset.columnUint64("TOTAL_RETRIES"));
  }
  // Job must NOT be in ARCHIVE_FAILED_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// When totalReportRetries reaches maxReportRetries the job must be moved atomically
// from ARCHIVE_ACTIVE_QUEUE to ARCHIVE_FAILED_QUEUE with STATUS=ReadyForDeletion.
TEST_P(RelationalDBTest, archiveFailReportBatchExceedRetries) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  // Force maxReportRetries=1 so the first report failure immediately exhausts the budget.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailReport("terminal report failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Job must be gone from ARCHIVE_ACTIVE_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  // Job must appear in ARCHIVE_FAILED_QUEUE with TOTAL_REPORT_RETRIES=1.
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT, MAX(TOTAL_REPORT_RETRIES) AS MAX_RR FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RR"));
  }
}

// A job with reportType=NoReportRequired must be treated identically to an
// exceed-retries job — it must be moved to ARCHIVE_FAILED_QUEUE regardless of
// how many report retries are left.
TEST_P(RelationalDBTest, archiveFailReportBatch_noReportRequired) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  // Override reportType in memory to NoReportRequired (default maxReportRetries=2, so
  // without this override the job would take the requeue path).
  jobs.front()->reportType = SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired;
  jobs.front()->prepareFailReport("no report required failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Must be moved to ARCHIVE_FAILED_QUEUE, not retried.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
}

// A single failReportBatch call may contain a mix of jobs: some exceed their report
// retry budget (→ ARCHIVE_FAILED_QUEUE) and some are merely requeued (→ IS_REPORTING=FALSE).
// Verify the SQL correctly routes them into the two paths within one transaction.
TEST_P(RelationalDBTest, archiveFailReportBatchMixed) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  queueArchiveJob(db, lc, 222, "tapePoolA", "A2", 2000);
  queueArchiveJob(db, lc, 333, "tapePoolA", "A3", 3000);
  db.waitSubthreadsComplete();

  // A1 gets maxReportRetries=1 so one failure exhausts the budget → exceed path.
  // A2 and A3 keep the default maxReportRetries=2 → requeue path.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1"
      " WHERE DISK_FILE_ID = 'diskFile-A1'");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(3u, jobs.size());

  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailReport("report failure");
    batch.push_back(job.get());
  }
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // A1 exceeded report retries → moved to ARCHIVE_FAILED_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // A2, A3 have retries left → still in ARCHIVE_ACTIVE_QUEUE with IS_REPORTING=FALSE.
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
}

// When a single copy of a multi-copy archive request exceeds its report retry budget,
// failReportBatchExceedRetries must atomically move ALL copies of the same archive
// request to ARCHIVE_FAILED_QUEUE (same semantics as the individual failReport path
// which calls moveJobToFailedQueueTable using ARCHIVE_REQUEST_ID for req_job_count > 1).
TEST_P(RelationalDBTest, archiveFailReportBatchExceedRetries_multiCopy) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // One 2-copy request: both copies go to tapePoolA.
  queueMultiCopyArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  // Force maxReportRetries=1 so the first report failure immediately exceeds the limit.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Fail only the FIRST copy (totalReportRetries=1 = maxReportRetries=1 → exceed path).
  // Because req_job_count=2, failReportBatchExceedRetries matches by ARCHIVE_REQUEST_ID
  // and must move BOTH copies to ARCHIVE_FAILED_QUEUE.
  auto& failedJob = jobs.front();
  failedJob->prepareFailReport("report failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {failedJob.get()};
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Both copies must be in ARCHIVE_FAILED_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  // No job must remain in ARCHIVE_ACTIVE_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// The failure reason passed to prepareFailReport must be persisted to the
// REPORT_FAILURE_LOG column so operators can diagnose why reporting retried.
TEST_P(RelationalDBTest, archiveFailReportBatch_persistsReportFailureLog) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueArchiveJob(db, lc, 111, "tapePoolA", "A1", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "TAPE001";
  tape.tapePool = "tapePoolA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  const std::string failureReason = "distinctive-report-failure-sentinel-99";
  jobs.front()->prepareFailReport(failureReason);
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  // requeue path (maxReportRetries=2, totalReportRetries=1 after prepareFailReport)
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT REPORT_FAILURE_LOG, FAILURE_LOG FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    const auto reportLog = rset.columnString("REPORT_FAILURE_LOG");
    ASSERT_NE(std::string::npos, reportLog.find(failureReason))
      << "REPORT_FAILURE_LOG does not contain the expected failure reason: " << reportLog;
    // FAILURE_LOG (transfer log) must be untouched — prepareFailReport only writes REPORT_FAILURE_LOG.
    auto transferLog = rset.columnOptionalString("FAILURE_LOG");
    ASSERT_FALSE(transferLog.has_value())
      << "FAILURE_LOG was unexpectedly written by prepareFailReport: " << transferLog.value_or("");
  }
}

// Helper: put a retrieve job into RJS_ToReportToUserForFailure status in RETRIEVE_ACTIVE_QUEUE
// and return the jobs batch ready for report-failure testing.
// maxReportRetriesOverride: if set, overrides MAX_REPORT_RETRIES in RETRIEVE_PENDING_QUEUE before
// getNextJobBatch moves rows to the active queue, so the in-memory value matches the DB.
static std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>
setupRetrieveJobForReportFailureTesting(cta::RelationalDBTestWrapper& db, cta::log::LogContext& lc,
                                        uint64_t archiveFileId = 333,
                                        std::optional<uint32_t> maxReportRetriesOverride = std::nullopt) {
  using namespace cta;
  queueRetrieveJob(db, lc, archiveFileId, "vidA", "A1", 3000);
  db.waitSubthreadsComplete();

  // Force exceed-retries-on-first-transfer-failure so the job reaches ToReportToUserForFailure
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }
  if (maxReportRetriesOverride.has_value()) {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = :V");
    stmt.bindUint32(":V", maxReportRetriesOverride.value());
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  for (auto& job : jobs) {
    job->prepareFailTransfer("initial transfer failure");
  }
  std::vector<SchedulerDatabase::RetrieveJob*> transferBatch;
  for (auto& job : jobs) transferBatch.push_back(job.get());
  mount->failTransferBatch(transferBatch, lc);
  // Job is now RJS_ToReportToUserForFailure in RETRIEVE_ACTIVE_QUEUE

  auto reportJobs = db.getNextRetrieveJobsToReportBatch(10, lc);
  return reportJobs;
}

// When totalReportRetries is below maxReportRetries, the job must stay in
// RETRIEVE_ACTIVE_QUEUE with IS_REPORTING=FALSE (requeue path).
TEST_P(RelationalDBTest, retrieveFailReportBatchRequeue) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  auto reportJobs = setupRetrieveJobForReportFailureTesting(db, lc);
  ASSERT_EQ(1u, reportJobs.size());

  // prepareFailReport increments totalReportRetries to 1 (default maxReportRetries=2 → requeue path)
  reportJobs.front()->prepareFailReport("report failure reason");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {reportJobs.front().get()};
  db.failRetrieveJobBatchReport(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Job must remain in RETRIEVE_ACTIVE_QUEUE with IS_REPORTING=FALSE, TOTAL_REPORT_RETRIES=1.
  {
    auto stmt = conn.createStmt(
      "SELECT TOTAL_REPORT_RETRIES, IS_REPORTING, TOTAL_RETRIES"
      " FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_REPORT_RETRIES"));
    ASSERT_FALSE(rset.columnBool("IS_REPORTING"));
    // prepareFailReport must NOT have touched TOTAL_RETRIES (transfer counter).
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
  }
  // Job must NOT be in RETRIEVE_FAILED_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// When totalReportRetries reaches maxReportRetries the job must be moved atomically
// from RETRIEVE_ACTIVE_QUEUE to RETRIEVE_FAILED_QUEUE.
TEST_P(RelationalDBTest, retrieveFailReportBatchExceedRetries) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // maxReportRetries=1: set in RETRIEVE_PENDING_QUEUE before getNextJobBatch so the in-memory
  // value loaded by getNextRetrieveJobsToReportBatch is 1 → first report failure exhausts it.
  auto reportJobs = setupRetrieveJobForReportFailureTesting(db, lc, 333, 1);
  ASSERT_EQ(1u, reportJobs.size());

  reportJobs.front()->prepareFailReport("terminal report failure");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {reportJobs.front().get()};
  db.failRetrieveJobBatchReport(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Job must be gone from RETRIEVE_ACTIVE_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  // Job must appear in RETRIEVE_FAILED_QUEUE with TOTAL_REPORT_RETRIES=1.
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT, MAX(TOTAL_REPORT_RETRIES) AS MAX_RR FROM RETRIEVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RR"));
  }
}

// A job with reportType=NoReportRequired must be treated identically to an
// exceed-retries job — it must be moved to RETRIEVE_FAILED_QUEUE regardless of
// how many report retries are left.
TEST_P(RelationalDBTest, retrieveFailReportBatch_noReportRequired) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  auto reportJobs = setupRetrieveJobForReportFailureTesting(db, lc);
  ASSERT_EQ(1u, reportJobs.size());

  // Override reportType in memory to NoReportRequired (default maxReportRetries=2, so
  // without this override the job would take the requeue path).
  reportJobs.front()->reportType = SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired;
  reportJobs.front()->prepareFailReport("no report required failure");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {reportJobs.front().get()};
  db.failRetrieveJobBatchReport(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Must be moved to RETRIEVE_FAILED_QUEUE, not retried.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
}

// A single failRetrieveJobBatchReport call may contain a mix of jobs: some exceed their
// report retry budget (→ RETRIEVE_FAILED_QUEUE) and some are merely requeued (IS_REPORTING=FALSE).
TEST_P(RelationalDBTest, retrieveFailReportBatchMixed) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // Queue 3 jobs on the same VID
  queueRetrieveJob(db, lc, 333, "vidA", "A1", 3000);
  queueRetrieveJob(db, lc, 334, "vidA", "A2", 3001);
  queueRetrieveJob(db, lc, 335, "vidA", "A3", 3002);
  db.waitSubthreadsComplete();

  // Force all to exceed transfer retries → all go to ToReportToUserForFailure
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }
  // A1 gets maxReportRetries=1 so one failure exhausts the budget → exceed path.
  // A2 and A3 keep the default maxReportRetries=2 → requeue path.
  // Must be set in RETRIEVE_PENDING_QUEUE before getNextJobBatch moves rows to the active
  // queue so the value loaded into memory by getNextRetrieveJobsToReportBatch is correct.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "UPDATE RETRIEVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1"
      " WHERE DISK_FILE_ID = 'diskFile-A1'");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(3u, jobs.size());
  for (auto& job : jobs) {
    job->prepareFailTransfer("initial transfer failure");
  }
  std::vector<SchedulerDatabase::RetrieveJob*> transferBatch;
  for (auto& job : jobs) transferBatch.push_back(job.get());
  mount->failTransferBatch(transferBatch, lc);

  auto reportJobs = db.getNextRetrieveJobsToReportBatch(10, lc);
  ASSERT_EQ(3u, reportJobs.size());

  std::vector<SchedulerDatabase::RetrieveJob*> reportBatch;
  for (auto& job : reportJobs) {
    job->prepareFailReport("report failure");
    reportBatch.push_back(job.get());
  }
  db.failRetrieveJobBatchReport(reportBatch, lc);

  auto conn = db.getRelationalDB().getConn();
  // A1 exceeded report retries → moved to RETRIEVE_FAILED_QUEUE.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // A2, A3 have retries left → still in RETRIEVE_ACTIVE_QUEUE with IS_REPORTING=FALSE.
  {
    auto stmt = conn.createStmt(
      "SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
}

// The failure reason passed to prepareFailReport must be persisted to the
// REPORT_FAILURE_LOG column; FAILURE_LOG (transfer log) must remain untouched.
TEST_P(RelationalDBTest, retrieveFailReportBatch_persistsReportFailureLog) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  auto reportJobs = setupRetrieveJobForReportFailureTesting(db, lc);
  ASSERT_EQ(1u, reportJobs.size());

  const std::string failureReason = "distinctive-retrieve-report-failure-sentinel-99";
  reportJobs.front()->prepareFailReport(failureReason);
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {reportJobs.front().get()};
  // requeue path (maxReportRetries=2, totalReportRetries=1 after prepareFailReport)
  db.failRetrieveJobBatchReport(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT REPORT_FAILURE_LOG, FAILURE_LOG FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    const auto reportLog = rset.columnString("REPORT_FAILURE_LOG");
    ASSERT_NE(std::string::npos, reportLog.find(failureReason))
      << "REPORT_FAILURE_LOG does not contain the expected failure reason: " << reportLog;
    // FAILURE_LOG (transfer log) must be non-empty from the initial failTransferBatch call,
    // but must NOT contain the report failure reason.
    const auto transferLog = rset.columnOptionalString("FAILURE_LOG");
    if (transferLog.has_value()) {
      ASSERT_EQ(std::string::npos, transferLog->find(failureReason))
        << "FAILURE_LOG was unexpectedly written by prepareFailReport: " << *transferLog;
    }
  }
}

// prepareFailReport must only increment TOTAL_REPORT_RETRIES, not TOTAL_RETRIES.
// After failTransferBatch (TOTAL_RETRIES=1) and one prepareFailReport (TOTAL_REPORT_RETRIES=1),
// both counters must be independently readable.
TEST_P(RelationalDBTest, retrieveFailReportBatch_totalRetriesUnchanged) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  auto reportJobs = setupRetrieveJobForReportFailureTesting(db, lc);
  ASSERT_EQ(1u, reportJobs.size());

  reportJobs.front()->prepareFailReport("report failure reason");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {reportJobs.front().get()};
  db.failRetrieveJobBatchReport(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT TOTAL_RETRIES, TOTAL_REPORT_RETRIES FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    // TOTAL_RETRIES was incremented by failTransferBatch (1 transfer failure).
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
    // TOTAL_REPORT_RETRIES was incremented only by prepareFailReport.
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_REPORT_RETRIES"));
  }
}

static cta::RelationalDBTestFactory RelationalDBTestFactoryStatic;
INSTANTIATE_TEST_CASE_P(RelationalDBTest,
                        RelationalDBTest,
                        ::testing::Values(RelationalDBTestParams(&RelationalDBTestFactoryStatic)));

}  // namespace unitTests

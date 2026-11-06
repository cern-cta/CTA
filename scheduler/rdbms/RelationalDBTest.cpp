/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/TapeForWriting.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <string>
#include <thread>
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

TEST_P(RelationalDBTest, repackArchiveJobStartTimeIsResetWhenRetrieveJobIsTransformed) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  // The repack queue tables carry a foreign key on REPACK_REQUEST_ID, so the repack request has
  // to exist before a repack retrieve job can be inserted below.
  const std::string vid = "V10000";
  auto mountPolicy = makeMountPolicy(1000);
  cta::SchedulerDatabase::QueueRepackRequest repackRequest(
    vid,
    "/repack/buffer",
    cta::common::dataStructures::RepackInfo::Type::MoveAndAddCopies,
    mountPolicy,
    false,
    10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);

  // Retrieving a repack tape takes hours, so by the time its retrieve jobs are transformed into
  // archive jobs they are already far older than any MIN_ARCHIVE_REQUEST_AGE.
  const uint64_t now = static_cast<uint64_t>(cta::utils::getCurrentEpochTime());
  const uint64_t retrieveStartTime = now - 100000;
  constexpr uint64_t creationTime = 500;

  {
    auto conn = db.getRelationalDB().getConn();

    uint64_t repackRequestId = 0;
    {
      auto stmt = conn.createStmt("SELECT REPACK_REQUEST_ID FROM REPACK_REQUEST_TRACKING WHERE VID = :VID");
      stmt.bindString(":VID", vid);
      auto rset = stmt.executeQuery();
      ASSERT_TRUE(rset.next());
      repackRequestId = rset.columnUint64("REPACK_REQUEST_ID");
    }

    // Insert the finished repack retrieve job directly: driving a repack retrieve all the way to
    // RJS_ToReportToRepackForSuccess through the scheduler would need a full tape session.
    // REPACK_REARCHIVE_COPY_NBS lists both the repacked copy (1, inserted by the transformation's
    // BASE_INSERT) and an additional copy (2, inserted by its ALTERNATE_INSERT), so that both
    // insert branches are covered.
    auto stmt = conn.createStmt(R"SQL(
      INSERT INTO REPACK_RETRIEVE_ACTIVE_QUEUE (
        REPACK_REQUEST_ID,
        RETRIEVE_REQUEST_ID,
        REQUEST_JOB_COUNT,
        STATUS,
        ARCHIVE_FILE_ID,
        CREATION_TIME,
        START_TIME,
        STORAGE_CLASS,
        SIZE_IN_BYTES,
        VID,
        TAPE_POOL,
        MOUNT_POLICY,
        PRIORITY,
        MIN_RETRIEVE_REQUEST_AGE,
        COPY_NB,
        MOUNT_ID,
        REPACK_REARCHIVE_COPY_NBS,
        REPACK_REARCHIVE_TAPE_POOLS
      ) VALUES (
        :REPACK_REQUEST_ID,
        777,
        1,
        :STATUS,
        555,
        :CREATION_TIME,
        :START_TIME,
        :STORAGE_CLASS,
        1000,
        :VID,
        'tapePoolA',
        'mountPolicy',
        1,
        3600,
        1,
        42,
        '1,2',
        'tapePoolA,tapePoolB'
      )
    )SQL");
    stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
    stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindUint64(":CREATION_TIME", creationTime);
    stmt.bindUint64(":START_TIME", retrieveStartTime);
    stmt.bindString(":STORAGE_CLASS", s_storageClassName);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  }
  // Let the connection go back to the pool before the transformation asks for its own ones.

  const uint64_t beforeTransform = static_cast<uint64_t>(cta::utils::getCurrentEpochTime());
  db.getNextSuccessfulRetrieveRepackReportBatch(lc);
  const uint64_t afterTransform = static_cast<uint64_t>(cta::utils::getCurrentEpochTime());

  auto conn = db.getRelationalDB().getConn();
  auto stmt = conn.createStmt(R"SQL(
    SELECT TAPE_POOL, START_TIME, CREATION_TIME
    FROM REPACK_ARCHIVE_PENDING_QUEUE
    ORDER BY COPY_NB
  )SQL");
  auto rset = stmt.executeQuery();

  std::list<std::string> tapePools;
  while (rset.next()) {
    tapePools.emplace_back(rset.columnString("TAPE_POOL"));
    // The age the mount decision sees has to be the age of the job in the archive queue: the
    // ARCHIVE_QUEUE_SUMMARY/REPACK_ARCHIVE_QUEUE_SUMMARY views compare MIN(START_TIME) against
    // MIN_ARCHIVE_REQUEST_AGE, so a START_TIME carried over from the retrieve job makes the
    // archive job look old enough to mount the moment it is queued. The stamp has to be the one
    // taken while the transformation ran: neither the retrieve job's own (far in the past) nor
    // anything in the future. Postgres rounds the EXTRACT(EPOCH ...)::BIGINT cast rather than
    // truncating it, so the upper bound allows for one second of rounding up.
    const uint64_t startTime = rset.columnUint64("START_TIME");
    ASSERT_GE(startTime, beforeTransform);
    ASSERT_LE(startTime, afterTransform + 1);
    // CREATION_TIME is not part of the age check and keeps tracking the original request.
    ASSERT_EQ(creationTime, rset.columnUint64("CREATION_TIME"));
  }
  // One archive job for the repacked copy (BASE_INSERT), one for the additional copy
  // (ALTERNATE_INSERT).
  ASSERT_EQ((std::list<std::string> {"tapePoolA", "tapePoolB"}), tapePools);
}

// Inserts a fixture row directly into REPACK_ARCHIVE_PENDING_QUEUE: driving a repack archive
// job to this point through the scheduler would need a full tape session (see the retrieve
// equivalent above). Only VID/DRIVE/HOST/MOUNT_TYPE/LOGICAL_LIBRARY/MOUNT_ID are omitted - those
// are always overwritten from the mount's own info by moveJobsToDbActiveQueue() when the job is
// dispatched, never carried over from PENDING.
void insertRepackArchivePendingJob(cta::rdbms::Conn& conn,
                                   uint64_t repackRequestId,
                                   uint64_t archiveFileId,
                                   const std::string& tapePool,
                                   uint64_t sizeInBytes,
                                   uint16_t totalRetries,
                                   uint16_t maxTotalRetries,
                                   uint16_t totalReportRetries,
                                   uint16_t maxReportRetries,
                                   uint16_t copyNb = 1,
                                   uint32_t requestJobCount = 1) {
  auto stmt = conn.createStmt(R"SQL(
    INSERT INTO REPACK_ARCHIVE_PENDING_QUEUE (
      REPACK_REQUEST_ID, ARCHIVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, TAPE_POOL, MOUNT_POLICY,
      PRIORITY, MIN_ARCHIVE_REQUEST_AGE, ARCHIVE_FILE_ID, SIZE_IN_BYTES, COPY_NB, START_TIME,
      CHECKSUMBLOB, CREATION_TIME, DISK_INSTANCE, DISK_FILE_ID, DISK_FILE_OWNER_UID, DISK_FILE_GID,
      DISK_FILE_PATH, ARCHIVE_REPORT_URL, ARCHIVE_ERROR_REPORT_URL, REQUESTER_NAME, REQUESTER_GROUP,
      SRC_URL, STORAGE_CLASS, RETRIES_WITHIN_MOUNT, MAX_RETRIES_WITHIN_MOUNT, TOTAL_RETRIES,
      LAST_MOUNT_WITH_FAILURE, MAX_TOTAL_RETRIES, TOTAL_REPORT_RETRIES, MAX_REPORT_RETRIES
    ) VALUES (
      :REPACK_REQUEST_ID, :ARCHIVE_REQUEST_ID, :REQUEST_JOB_COUNT, :STATUS, :TAPE_POOL, 'mountPolicy',
      1, 0, :ARCHIVE_FILE_ID, :SIZE_IN_BYTES, :COPY_NB, :START_TIME,
      :CHECKSUMBLOB, :CREATION_TIME, 'diskInstance', :DISK_FILE_ID, 9751, 9752,
      '/repack/path', 'test://archive-report-url', 'test://error-report-url', 'user', 'group',
      :SRC_URL, 'storageClass', 0, :MAX_RETRIES_WITHIN_MOUNT, :TOTAL_RETRIES,
      0, :MAX_TOTAL_RETRIES, :TOTAL_REPORT_RETRIES, :MAX_REPORT_RETRIES
    )
  )SQL");
  stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
  // No FK on ARCHIVE_REQUEST_ID in this queue - reusing archiveFileId keeps the fixture simple.
  stmt.bindUint64(":ARCHIVE_REQUEST_ID", archiveFileId);
  stmt.bindUint32(":REQUEST_JOB_COUNT", requestJobCount);
  stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack));
  stmt.bindString(":TAPE_POOL", tapePool);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.bindUint64(":SIZE_IN_BYTES", sizeInBytes);
  stmt.bindUint16(":COPY_NB", copyNb);
  stmt.bindUint64(":START_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()));
  stmt.bindBlob(":CHECKSUMBLOB", cta::checksum::ChecksumBlob().serialize());
  stmt.bindUint64(":CREATION_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()));
  stmt.bindString(":DISK_FILE_ID", "diskFile-" + std::to_string(archiveFileId));
  stmt.bindString(":SRC_URL", "root://disk/repack-" + std::to_string(archiveFileId));
  stmt.bindUint16(":MAX_RETRIES_WITHIN_MOUNT", maxTotalRetries);
  stmt.bindUint16(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint16(":MAX_TOTAL_RETRIES", maxTotalRetries);
  stmt.bindUint16(":TOTAL_REPORT_RETRIES", totalReportRetries);
  stmt.bindUint16(":MAX_REPORT_RETRIES", maxReportRetries);
  stmt.executeNonQuery();
}

// Finds the ArchiveForRepack potential mount for tapePool (requires a matching row already
// queued, e.g. via insertRepackArchivePendingJob()) and creates a real repack archive mount on
// destVid, exactly as a tape server would - so the job objects returned by its getNextJobBatch()
// have isRepack set the same way production does (ArchiveMount::setIsRepack() ->
// ArchiveRdbJob::initialize()), not hand-constructed.
std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> createRepackArchiveMount(cta::SchedulerDatabase& db,
                                                                               cta::log::LogContext& lc,
                                                                               const std::string& tapePool,
                                                                               const std::string& destVid) {
  using namespace cta;
  auto mountInfo = db.getMountInfo(lc);
  const SchedulerDatabase::PotentialMount* repackMount = nullptr;
  for (const auto& pm : mountInfo->potentialMounts) {
    if (pm.type == common::dataStructures::MountType::ArchiveForRepack && pm.tapePool == tapePool) {
      repackMount = &pm;
      break;
    }
  }
  if (repackMount == nullptr) {
    throw cta::exception::Exception(
      "In createRepackArchiveMount(): no ArchiveForRepack potential mount found for tapePool " + tapePool);
  }

  catalogue::TapeForWriting tape;
  tape.vid = destVid;
  tape.tapePool = tapePool;
  tape.vo = "vo";
  tape.mediaType = "T10k";
  tape.vendor = "Oracle";
  tape.capacityInBytes = 1'000'000'000ULL;
  tape.labelFormat = common::dataStructures::Label::Format::CTA;

  return mountInfo->createArchiveMount(*repackMount, tape, "drive", "library", "host");
}

uint64_t getRepackRequestId(cta::RelationalDB& relationalDb, const std::string& vid) {
  auto conn = relationalDb.getConn();
  auto stmt = conn.createStmt("SELECT REPACK_REQUEST_ID FROM REPACK_REQUEST_TRACKING WHERE VID = :VID");
  stmt.bindString(":VID", vid);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw cta::exception::Exception("In getRepackRequestId(): no REPACK_REQUEST_TRACKING row for VID " + vid);
  }
  return rset.columnUint64("REPACK_REQUEST_ID");
}

// Covers: repack transfer fails with retries left - requeued, not reported as a failure yet.
// Also covers a repack catalogue-update failure: MigrationReportPacker.cpp routes that through
// failTransfer() too (only FailedReportMoveToQueue reaches failReport(), see the
// bookkeeping-recovery test below), so this one test exercises both scenarios.
TEST_P(RelationalDBTest, repackArchiveTransferFailureIsRequeuedBeforeExceedingRetries) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-RETRY";
  const std::string tapePool = "repackTapePoolRetry";
  auto mountPolicy = makeMountPolicy(1000);
  SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                      "/repack/buffer",
                                                      common::dataStructures::RepackInfo::Type::MoveOnly,
                                                      mountPolicy,
                                                      false,
                                                      10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t archiveFileId = 990001;
  constexpr uint64_t sizeInBytes = 5000;
  {
    auto conn = db.getRelationalDB().getConn();
    // One retry below its limit: this failure must be requeued, not reported as terminal.
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePool,
                                  sizeInBytes,
                                  /*totalRetries=*/0,
                                  /*maxTotalRetries=*/2,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-RETRY-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  // Simulate the physical transfer failing, exactly as MigrationReportPacker's
  // FailedReportCatalogueUpdate/transfer-error handling calls failTransfer() on the job.
  jobs.front()->failTransfer("simulated transfer failure", lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt(
      "SELECT STATUS, TOTAL_RETRIES FROM REPACK_ARCHIVE_PENDING_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack), rset.columnString("STATUS"));
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
    ASSERT_FALSE(rset.next());
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }

  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(0u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(0u, repackInfo.failedBytesToArchive);
}

// Covers: "repack transfer fails on its last retry" - must be reported as a failure, moved to
// REPACK_ARCHIVE_FAILED_QUEUE, and correctly accounted for on the repack request.
TEST_P(RelationalDBTest, repackArchiveTransferFailureExceedsRetriesMovesToFailedQueueAndAccounts) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-EXCEED";
  const std::string tapePool = "repackTapePoolExceed";
  auto mountPolicy = makeMountPolicy(1000);
  SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                      "/repack/buffer",
                                                      common::dataStructures::RepackInfo::Type::MoveOnly,
                                                      mountPolicy,
                                                      false,
                                                      10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t archiveFileId = 990002;
  constexpr uint64_t sizeInBytes = 7000;
  {
    auto conn = db.getRelationalDB().getConn();
    // Already on its last retry: this failure must be terminal.
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePool,
                                  sizeInBytes,
                                  /*totalRetries=*/1,
                                  /*maxTotalRetries=*/2,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-EXCEED-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->failTransfer("simulated terminal transfer failure", lc);

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT STATUS FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForFailure),
              rset.columnString("STATUS"));
  }

  // maintd's RepackReportRoutine equivalent: drains the failed-report batch, moving the row to
  // REPACK_ARCHIVE_FAILED_QUEUE and crediting the repack request's failure statistics.
  db.getNextFailedArchiveRepackReportBatch(lc);

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("C"));
  }

  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(1u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(sizeInBytes, repackInfo.failedBytesToArchive);
}

// Covers: repack transfer and catalogue update both succeed, but the write recording that in
// the scheduler DB fails - failReport() is only reachable once the catalogue write already
// committed (ArchiveMount::reportJobsBatchTransferred()'s catalogue_updated gate). This is the
// bug fixed in updateJobStatusForFailedReport(): it must recover the lost write (row ends up
// ReadyForDeletion) rather than no-op against the wrong table or mislabel a success as a failure.
TEST_P(RelationalDBTest, repackArchiveBookkeepingReportFailureRecoversAsSuccessAndAccounts) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BKFAIL";
  const std::string tapePool = "repackTapePoolBkFail";
  auto mountPolicy = makeMountPolicy(1000);
  SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                      "/repack/buffer",
                                                      common::dataStructures::RepackInfo::Type::MoveOnly,
                                                      mountPolicy,
                                                      false,
                                                      10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t archiveFileId = 990003;
  constexpr uint64_t sizeInBytes = 9000;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePool,
                                  sizeInBytes,
                                  /*totalRetries=*/0,
                                  /*maxTotalRetries=*/2,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BKFAIL-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  // Simulate the scheduler-DB bookkeeping write failing after a successful transfer by calling
  // failReport() directly, exactly as MigrationReportPacker's FailedReportMoveToQueue catch does.
  jobs.front()->failReport("simulated scheduler bookkeeping write failure", lc);

  {
    // Recovered, not failed out: still in ACTIVE, ReadyForDeletion.
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT STATUS FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion), rset.columnString("STATUS"));
  }
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }

  // maintd's RepackReportRoutine equivalent: drains the successful-report batch, deleting the
  // row and crediting the repack request's progress statistics.
  db.getNextSuccessfulArchiveRepackReportBatch(lc);

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }

  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(1u, repackInfo.archivedFiles);
  ASSERT_EQ(sizeInBytes, repackInfo.archivedBytes);
}

// Covers the multi-copy case of the same bug: updateRepackJobSuccess() only moves ALL copies of
// a REQUEST_JOB_COUNT > 1 request to ReadyForDeletion once every copy has reported success -
// until then it marks just the reporting copy AJS_ToReportToRepackForSuccess. Recovering copy
// 1's failure must not jump straight to ReadyForDeletion while copy 2 hasn't transferred yet,
// or getNextSuccessfulArchiveRepackReportBatch() would delete the source file prematurely.
TEST_P(RelationalDBTest, repackArchiveBookkeepingReportFailureRecoversMultiCopyCorrectly) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-MULTICOPY";
  const std::string tapePoolA = "repackTapePoolMultiCopyA";
  const std::string tapePoolB = "repackTapePoolMultiCopyB";
  auto mountPolicy = makeMountPolicy(1000);
  SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                      "/repack/buffer",
                                                      common::dataStructures::RepackInfo::Type::MoveAndAddCopies,
                                                      mountPolicy,
                                                      false,
                                                      10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  // Two copies of the same logical file (same ARCHIVE_FILE_ID), one per tape pool - the usual
  // shape for a MoveAndAddCopies repack destination.
  constexpr uint64_t archiveFileId = 990004;
  constexpr uint64_t sizeInBytes = 11000;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePoolA,
                                  sizeInBytes,
                                  /*totalRetries=*/0,
                                  /*maxTotalRetries=*/2,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/2,
                                  /*copyNb=*/1,
                                  /*requestJobCount=*/2);
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePoolB,
                                  sizeInBytes,
                                  /*totalRetries=*/0,
                                  /*maxTotalRetries=*/2,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/2,
                                  /*copyNb=*/2,
                                  /*requestJobCount=*/2);
  }

  // Copy 1 transfers and its catalogue update succeeds, but the scheduler-DB bookkeeping write
  // fails. Copy 2 has not even been dispatched yet.
  auto mountA = createRepackArchiveMount(db, lc, tapePoolA, "V-MULTICOPY-DESTA");
  auto jobsA = mountA->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  jobsA.front()->failReport("simulated scheduler bookkeeping write failure for copy 1", lc);

  {
    // Copy 1: recovered to the intermediate "waiting for siblings" status, NOT ReadyForDeletion,
    // and NOT deleted - the request is not complete yet.
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "SELECT STATUS FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND COPY_NB = 1");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForSuccess),
              rset.columnString("STATUS"));
  }
  {
    // Copy 2: still sitting in PENDING, completely untouched by copy 1's recovery.
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "SELECT STATUS FROM REPACK_ARCHIVE_PENDING_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND COPY_NB = 2");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack), rset.columnString("STATUS"));
  }
  {
    // Nothing credited or deleted yet - the request is still incomplete.
    const auto repackInfoMidway = db.getRepackInfo(vid);
    ASSERT_EQ(0u, repackInfoMidway.archivedFiles);
    ASSERT_EQ(0u, repackInfoMidway.archivedBytes);
  }

  // Now copy 2 transfers, its catalogue update succeeds, and its scheduler-DB bookkeeping write
  // fails too - this is the last copy, so the recovery must complete the whole request.
  auto mountB = createRepackArchiveMount(db, lc, tapePoolB, "V-MULTICOPY-DESTB");
  auto jobsB = mountB->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());
  jobsB.front()->failReport("simulated scheduler bookkeeping write failure for copy 2", lc);

  {
    // Both copies are now ReadyForDeletion together.
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "SELECT COPY_NB, STATUS FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "ORDER BY COPY_NB");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    for (int expectedCopyNb : {1, 2}) {
      ASSERT_TRUE(rset.next());
      ASSERT_EQ(static_cast<uint64_t>(expectedCopyNb), rset.columnUint64("COPY_NB"));
      ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion), rset.columnString("STATUS"));
    }
    ASSERT_FALSE(rset.next());
  }

  // maintd's RepackReportRoutine equivalent: drains the successful-report batch, deleting both
  // copies and crediting the repack request's progress statistics once per copy.
  db.getNextSuccessfulArchiveRepackReportBatch(lc);

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }

  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(2u, repackInfo.archivedFiles);
  ASSERT_EQ(2 * sizeInBytes, repackInfo.archivedBytes);
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

// Regression test: createRetrieveMount() must write nextMountType to the catalogue
// BEFORE releasing the per-library advisory lock, so that a concurrent scheduler
// reading drive state while the lock is held sees the mount as pending and cannot
// over-schedule beyond the VO readMaxDrives limit.
TEST_P(RelationalDBTest, createRetrieveMountSetsNextMountTypeBeforeLockRelease) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  queueRetrieveJob(getDb(), lc, 333, "vidA", "A", 3000);
  getDb().waitSubthreadsComplete();

  auto mountInfo = getDb().getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  const auto& potentialMount = mountInfo->potentialMounts.front();
  ASSERT_EQ(common::dataStructures::MountType::Retrieve, potentialMount.type);
  ASSERT_EQ("vidA", potentialMount.vid);

  mountInfo->createRetrieveMount(potentialMount, "drive", "library", "host");

  // After createRetrieveMount() the catalogue drive state must already reflect the
  // pending mount — this is the invariant that closes the VO-limit race window.
  auto& catalogue = getDb().getCatalogue();
  const auto optDrive = catalogue.DriveState()->getTapeDrive("drive");
  ASSERT_TRUE(optDrive.has_value());
  ASSERT_EQ(common::dataStructures::MountType::Retrieve, optDrive->nextMountType);
  ASSERT_EQ("vidA", optDrive->nextVid.value_or(""));
}

// Regression test: createArchiveMount() must write nextMountType to the catalogue
// BEFORE releasing the per-library advisory lock (symmetric to the retrieve case).
TEST_P(RelationalDBTest, createArchiveMountSetsNextMountTypeBeforeLockRelease) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);

  queueArchiveJob(getDb(), lc, 111, "tapePoolA", "A", 1000);
  getDb().waitSubthreadsComplete();

  auto mountInfo = getDb().getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  const auto& potentialMount = mountInfo->potentialMounts.front();
  ASSERT_EQ(common::dataStructures::MountType::ArchiveForUser, potentialMount.type);
  ASSERT_EQ("tapePoolA", potentialMount.tapePool);

  catalogue::TapeForWriting tape;
  tape.vid = "vidB";
  tape.tapePool = potentialMount.tapePool;
  tape.vo = "vo";
  tape.mediaType = "T10k";
  tape.vendor = "Oracle";
  tape.capacityInBytes = 1'000'000'000ULL;
  tape.labelFormat = common::dataStructures::Label::Format::CTA;

  mountInfo->createArchiveMount(potentialMount, tape, "drive", "library", "host");

  // After createArchiveMount() the catalogue drive state must already reflect the
  // pending mount — this is the invariant that closes the VO-limit race window.
  auto& catalogue = getDb().getCatalogue();
  const auto optDrive = catalogue.DriveState()->getTapeDrive("drive");
  ASSERT_TRUE(optDrive.has_value());
  ASSERT_EQ(common::dataStructures::MountType::ArchiveForUser, optDrive->nextMountType);
  ASSERT_EQ("vidB", optDrive->nextVid.value_or(""));
}

// Regression test: a mount busy reporting an already-fetched batch must not look inactive to
// the dead-mount reclaim routine for not having fetched new work recently -
// refreshMountLivenessHeartbeat() must also run on report, not just on fetch. Before the fix,
// the window between a file being written and the report's commit let the reclaim routine
// grab and re-queue the job, causing a duplicate re-transfer.
TEST_P(RelationalDBTest, archiveMountHeartbeatRefreshedOnReportNotJustFetch) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string tapePool = "heartbeatTapePool";
  queueArchiveJob(db, lc, 991000, tapePool, "HB", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  const auto& potentialMount = mountInfo->potentialMounts.front();
  ASSERT_EQ(common::dataStructures::MountType::ArchiveForUser, potentialMount.type);

  catalogue::TapeForWriting tape;
  tape.vid = "V-HEARTBEAT";
  tape.tapePool = tapePool;
  tape.vo = "vo";
  tape.mediaType = "T10k";
  tape.vendor = "Oracle";
  tape.capacityInBytes = 1'000'000'000ULL;
  tape.labelFormat = common::dataStructures::Label::Format::CTA;

  auto mount = mountInfo->createArchiveMount(potentialMount, tape, "drive", "library", "host");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());
  const uint64_t mountId = mount->getMountInfo().mountId;

  // refreshMountLivenessHeartbeat() self-throttles to one DB round-trip per 5s, tracked by an
  // in-process atomic on the mount object (m_lastHeartbeatRefreshEpoch) - not by reading the DB
  // row back. Faking staleness by writing MOUNT_QUEUE_LAST_FETCH directly would leave that
  // atomic still "fresh", so the report call below would skip its own round-trip as a no-op and
  // never actually refresh the row - wait out the real window instead, exactly as a mount that
  // spent a long time writing/reporting since its last fetch would.
  std::this_thread::sleep_for(std::chrono::seconds(6));

  // Sanity check: 6s since the only heartbeat (the fetch above) already exceeds this 3s
  // gc_delay, so without a report-time refresh this mount is already a dead-mount candidate.
  {
    auto deadBefore = db.getRelationalDB().getDeadMounts(3, lc);
    ASSERT_NE(deadBefore.archiveActive.end(),
              std::find(deadBefore.archiveActive.begin(), deadBefore.archiveActive.end(), mountId));
  }

  // The report call must refresh the heartbeat too, independently of fetch cadence.
  mount->setJobBatchTransferred(jobs, lc);

  {
    auto deadAfter = db.getRelationalDB().getDeadMounts(3, lc);
    ASSERT_EQ(deadAfter.archiveActive.end(),
              std::find(deadAfter.archiveActive.begin(), deadAfter.archiveActive.end(), mountId));
  }
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

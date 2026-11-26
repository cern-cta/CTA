/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDBTestFixture.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <utility>

// This file now holds only the test cases that don't fit a more specific RelationalDB*Test.cpp -
// shared fixture, queueing helpers and consts moved to RelationalDBTestFixture.hpp/.cpp so the
// suite can be split by topic across files instead of growing this one without bound. See e.g.
// RelationalDBMultiCopySettlementTest.cpp for the archive user multi-copy settlement cases.

namespace unitTests {

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

  // MoveAndAddCopies: the recalled copy (1) IS itself a rearchive target (it is being moved), so
  // it must still be credited as retrieved - confirms RETRIEVED_FILE_STATS (added to fix
  // AddCopiesOnly, where the recalled copy never is a target) did not change this case, which
  // used to derive the same count from BASE_INSERTED_COUNT instead.
  {
    auto stmt2 = conn.createStmt("SELECT RETRIEVED_FILES FROM REPACK_REQUEST_TRACKING WHERE VID = :VID");
    stmt2.bindString(":VID", vid);
    auto rset2 = stmt2.executeQuery();
    ASSERT_TRUE(rset2.next());
    ASSERT_EQ(1u, rset2.columnUint64("RETRIEVED_FILES"));
  }
}

// AddCopiesOnly rows never list their recalled copy in REPACK_REARCHIVE_COPY_NBS (read, not
// rearchived), so BASE_INSERT fires for neither row below. Deriving retrieve progress from
// BASE_INSERTED_COUNT therefore credited zero, and the user-provided (never-mounted) row in the
// same batch underflowed the unsigned subtraction, failing the BIGINT bind with "value out of
// range for type bigint".
TEST_P(RelationalDBTest, repackAddCopiesOnlyRetrieveProgressCountsCorrectlyAndDoesNotOverflow) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-ADDCOPIES";
  auto mountPolicy = makeMountPolicy(1000);
  cta::SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                           "/repack/buffer",
                                                           cta::common::dataStructures::RepackInfo::Type::AddCopiesOnly,
                                                           mountPolicy,
                                                           false,
                                                           10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);

  // getRepackRequestId() is defined later in this file, so it is not yet visible here - look the
  // ID up directly, the same way repackArchiveJobStartTimeIsResetWhenRetrieveJobIsTransformed()
  // above does.
  uint64_t repackRequestId = 0;
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("SELECT REPACK_REQUEST_ID FROM REPACK_REQUEST_TRACKING WHERE VID = :VID");
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    repackRequestId = rset.columnUint64("REPACK_REQUEST_ID");
  }

  {
    auto conn = db.getRelationalDB().getConn();
    // Row 1: a genuinely retrieved file (MOUNT_ID set, as a real tape mount would leave it).
    // Row 2: a user-provided file (MOUNT_ID NULL, pre-staged rather than recalled from tape) -
    // the combination that used to trigger the BIGINT overflow.
    // Two distinct placeholder names per shared value (:REPACK_REQUEST_ID/2, :STATUS/2,
    // :STORAGE_CLASS/2, :VID/2) - ParamNameToIdx rejects a repeated :name as a duplicate even
    // across two different VALUES tuples of the same INSERT, so each textual occurrence needs its
    // own name, not just a stylistic preference.
    auto stmt = conn.createStmt(R"SQL(
      INSERT INTO REPACK_RETRIEVE_ACTIVE_QUEUE (
        REPACK_REQUEST_ID, RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
        CREATION_TIME, START_TIME, STORAGE_CLASS, SIZE_IN_BYTES, VID, TAPE_POOL, MOUNT_POLICY,
        PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, MOUNT_ID, REPACK_REARCHIVE_COPY_NBS,
        REPACK_REARCHIVE_TAPE_POOLS
      ) VALUES
      (:REPACK_REQUEST_ID, 881, 1, :STATUS, 991, 500, 500, :STORAGE_CLASS, 1000, :VID, 'tapePoolA',
       'mountPolicy', 1, 3600, 1, 42, '2', 'tapePoolB'),
      (:REPACK_REQUEST_ID2, 882, 1, :STATUS2, 992, 500, 500, :STORAGE_CLASS2, 2000, :VID2, 'tapePoolA',
       'mountPolicy', 1, 3600, 1, NULL, '2', 'tapePoolB')
    )SQL");
    stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
    stmt.bindUint64(":REPACK_REQUEST_ID2", repackRequestId);
    stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindString(":STATUS2", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindString(":STORAGE_CLASS", s_storageClassName);
    stmt.bindString(":STORAGE_CLASS2", s_storageClassName);
    stmt.bindString(":VID", vid);
    stmt.bindString(":VID2", vid);
    stmt.executeNonQuery();
  }

  // Before the fix, this call throws "value out of range for type bigint" instead of returning.
  ASSERT_NO_THROW(db.getNextSuccessfulRetrieveRepackReportBatch(lc));

  auto conn = db.getRelationalDB().getConn();
  {
    // Neither row's own copy (1) is a rearchive target, so BASE_INSERT must write nothing -
    // both archive jobs come from ALTERNATE_INSERT, for copy 2.
    auto stmt = conn.createStmt("SELECT COPY_NB, COUNT(*) AS C FROM REPACK_ARCHIVE_PENDING_QUEUE GROUP BY COPY_NB");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(2u, rset.columnUint64("C"));
    ASSERT_FALSE(rset.next());
  }
  {
    // Only the genuinely retrieved row (row 1) counts - the user-provided one (row 2) must not,
    // and must not have corrupted the count via the old subtraction either.
    auto stmt = conn.createStmt("SELECT RETRIEVED_FILES FROM REPACK_REQUEST_TRACKING WHERE REPACK_REQUEST_ID = :ID");
    stmt.bindUint64(":ID", repackRequestId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("RETRIEVED_FILES"));
  }
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

// Both archive copies emitted by the MoveAndAddCopies transform must carry the full group size.
// The failure sweep uses that count to collect the group after one copy fails and the other
// settles through single-job report recovery.
TEST_P(RelationalDBTest, repackRetrieveToArchiveTransformProducesCorrectRequestJobCountForMultiCopy) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-XFORM-COUNT";
  const std::string tapePoolMoved = "repackTapePoolXformMoved";
  const std::string tapePoolAdded = "repackTapePoolXformAdded";
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
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t archiveFileId = 990006;
  constexpr uint64_t sizeInBytes = 13000;
  {
    // Insert the finished repack retrieve job directly, exactly as production's
    // Scheduler::expandRepackRequest()/RetrieveRequest::insert() would have left it: copy 1 is
    // the moved copy (retrieved from this VID), copy 2 is the added copy - both in
    // REPACK_REARCHIVE_COPY_NBS, per the comment above.
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(R"SQL(
      INSERT INTO REPACK_RETRIEVE_ACTIVE_QUEUE (
        REPACK_REQUEST_ID, RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
        CREATION_TIME, START_TIME, STORAGE_CLASS, SIZE_IN_BYTES, VID, TAPE_POOL, MOUNT_POLICY,
        PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, MOUNT_ID, REPACK_REARCHIVE_COPY_NBS,
        REPACK_REARCHIVE_TAPE_POOLS
      ) VALUES (
        :REPACK_REQUEST_ID, 778, 1, :STATUS, :ARCHIVE_FILE_ID, 500, :START_TIME, :STORAGE_CLASS,
        :SIZE_IN_BYTES, :VID, :TAPE_POOL_MOVED, 'mountPolicy', 1, 3600, 1, 42, '1,2',
        :TAPE_POOLS_REARCHIVE
      )
    )SQL");
    stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
    stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    stmt.bindUint64(":START_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()) - 100000);
    stmt.bindString(":STORAGE_CLASS", s_storageClassName);
    stmt.bindUint64(":SIZE_IN_BYTES", sizeInBytes);
    stmt.bindString(":VID", vid);
    stmt.bindString(":TAPE_POOL_MOVED", tapePoolMoved);
    stmt.bindString(":TAPE_POOLS_REARCHIVE", tapePoolMoved + "," + tapePoolAdded);
    stmt.executeNonQuery();
  }

  db.getNextSuccessfulRetrieveRepackReportBatch(lc);

  {
    // Both the moved copy (BASE_INSERT) and the added copy (ALTERNATE_INSERT) must carry
    // REQUEST_JOB_COUNT=2 - the real total, not 1 (which an off-by-one missing the moved copy
    // would produce).
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("SELECT COPY_NB, REQUEST_JOB_COUNT, TAPE_POOL FROM REPACK_ARCHIVE_PENDING_QUEUE "
                                "WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID ORDER BY COPY_NB");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(2u, rset.columnUint64("REQUEST_JOB_COUNT"));
    ASSERT_EQ(tapePoolMoved, rset.columnString("TAPE_POOL"));
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(2u, rset.columnUint64("REQUEST_JOB_COUNT"));
    ASSERT_EQ(tapePoolAdded, rset.columnString("TAPE_POOL"));
    ASSERT_FALSE(rset.next());
  }

  // Continue into a real multi-copy failure: the moved copy's transfer permanently fails while
  // the added copy's transfer succeeds.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE REPACK_ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountMoved = createRepackArchiveMount(db, lc, tapePoolMoved, "V-XFORM-MOVED-DST");
  auto jobsMoved = mountMoved->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsMoved.size());
  // Simulate the physical transfer failing exactly as MigrationReportPacker does: accumulate via
  // prepareFailTransfer() (in-memory only), then flush through ArchiveMount::failTransferBatch().
  jobsMoved.front()->prepareFailTransfer("simulated terminal transfer failure for moved copy");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobsMoved.front().get()};
    mountMoved->failTransferBatch(failBatch, lc);
  }

  // The added copy's transfer succeeds. Driving a real repack transfer success all the way
  // through reportJobsBatchTransferred() would need a full tape session (see this file's other
  // repack tests); calling prepareFailReport()+failReportBatch() directly on a freshly-fetched
  // job simulates the same outcome via its recovery path instead - for repack, reportType is
  // always NoReportRequired at this point (see ArchiveMount::failReportBatch()'s own comment), so
  // this always resolves through updateRepackJobSuccess(), i.e. a success, regardless of the
  // reason string.
  auto mountAdded = createRepackArchiveMount(db, lc, tapePoolAdded, "V-XFORM-ADDED-DST");
  auto jobsAdded = mountAdded->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsAdded.size());
  jobsAdded.front()->prepareFailReport("simulated scheduler bookkeeping write failure for added copy");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobsAdded.front().get()};
    mountAdded->failReportBatch(failBatch, lc);
  }

  db.getNextFailedArchiveRepackReportBatch(lc);

  {
    // Both copies end up in REPACK_ARCHIVE_FAILED_QUEUE together - the group's outcome is
    // "failed" as a whole, correctly computed against the transform-produced REQUEST_JOB_COUNT=2.
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("C"));
  }
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }
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

// ArchiveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable() picks a single-copy failure
// and a complete multi-copy group's failures straight off independent code paths (see that
// function's own comment): a single-copy row moves the instant it is failed, with no join at
// all, while a multi-copy group's eligibility is decided over every currently-failed multi-copy
// row before :LIMIT is ever applied, so one group's completeness can never depend on another,
// unrelated group's batch-window placement. This exercises all three outcomes in one call: a
// failed single-copy job moves immediately; a failed multi-copy group whose sibling has ALSO
// failed (complete) moves as a pair; a failed multi-copy group whose sibling is still mid-transfer
// (incomplete) is left exactly where it is - neither lost nor moved early.
TEST_P(RelationalDBTest, repackMoveFailedBatchHandlesSingleAndMultiCopyGroupsIndependently) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-MOVEFAILED";
  const std::string tapePool = "repackTapePoolMoveFailed";
  // The incomplete group's second copy is queued to a tape pool no mount is ever created for in
  // this test, so it is guaranteed to stay undispatched - deterministically, rather than relying
  // on moveJobsToDbActiveQueue()'s PRIORITY/ARCHIVE_FILE_ID ordering to leave the right row
  // behind when dispatching a partial batch out of a single shared pool.
  const std::string heldTapePool = "repackTapePoolMoveFailedHeld";
  auto mountPolicy = makeMountPolicy(1000);
  cta::SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                           "/repack/buffer",
                                                           cta::common::dataStructures::RepackInfo::Type::MoveOnly,
                                                           mountPolicy,
                                                           false,
                                                           10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t singleCopyFileId = 995001;
  constexpr uint64_t completeGroupFileId = 995002;
  constexpr uint64_t incompleteGroupFileId = 995003;
  {
    auto conn = db.getRelationalDB().getConn();
    // Single-copy failure: REQUEST_JOB_COUNT = 1, no sibling at all.
    insertRepackArchivePendingJob(conn, repackRequestId, singleCopyFileId, tapePool, 1000, 0, 1, 0, 5, 1, 1);
    // Complete multi-copy group: both copies will be dispatched and failed below.
    insertRepackArchivePendingJob(conn, repackRequestId, completeGroupFileId, tapePool, 1000, 0, 1, 0, 5, 1, 2);
    insertRepackArchivePendingJob(conn, repackRequestId, completeGroupFileId, tapePool, 1000, 0, 1, 0, 5, 2, 2);
    // Incomplete multi-copy group: copy 1 will be dispatched and failed; copy 2 is queued to the
    // held tape pool, never dispatched.
    insertRepackArchivePendingJob(conn, repackRequestId, incompleteGroupFileId, tapePool, 1000, 0, 1, 0, 5, 1, 2);
    insertRepackArchivePendingJob(conn, repackRequestId, incompleteGroupFileId, heldTapePool, 1000, 0, 1, 0, 5, 2, 2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-MOVEFAILED-DST");

  // Dispatch everything queued to this mount's own tape pool - 4 of the 5 pending rows.
  auto jobs = mount->getNextJobBatch(4, 1'000'000, lc);
  ASSERT_EQ(4u, jobs.size());

  {
    // The held pool's row is still sitting in REPACK_ARCHIVE_PENDING_QUEUE, untouched.
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM REPACK_ARCHIVE_PENDING_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // Fail every dispatched job - one at a time, mirroring MigrationReportPacker's own
  // accumulate-then-flush pattern.
  for (auto& job : jobs) {
    job->prepareFailTransfer("simulated terminal transfer failure for moveFailedRepackJobBatchToFailedQueueTable "
                             "independence test");
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {job.get()};
    mount->failTransferBatch(failBatch, lc);
  }

  db.getNextFailedArchiveRepackReportBatch(lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // Single-copy job and the complete group's two copies all moved - 3 rows total.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(3u, rset.columnUint64("C"));
  }
  {
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID IN (:A, :B)");
    stmt.bindUint64(":A", singleCopyFileId);
    stmt.bindUint64(":B", completeGroupFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(3u, rset.columnUint64("C"));
  }
  {
    // The incomplete group's failed copy is left behind, still failed, still visible - not lost
    // and not swept early.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                                "WHERE ARCHIVE_FILE_ID = :A AND STATUS = 'AJS_ToReportToRepackForFailure'");
    stmt.bindUint64(":A", incompleteGroupFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("C"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID = :A");
    stmt.bindUint64(":A", incompleteGroupFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }
}

// ArchiveJobQueueRow::updateRepackJobSuccess() must never demote a row that has already reached
// ReadyForDeletion back to AJS_ToReportToRepackForSuccess. A caller can replay the same success
// more than once - report recovery (failReportBatch()'s repack "exceed" branch, which always
// calls this function, see that function's own comment) replaying a write whose first attempt
// actually went through is exactly that scenario. Before the monotonicity guard, a replay would
// find its own row via the plain JOB_ID match (no STATUS check), see its own group as
// "incomplete" (the sibling's STATUS is also already ReadyForDeletion, not
// AJS_ToReportToRepackForSuccess, so it would not be counted), and demote the row while its
// sibling stayed at ReadyForDeletion - stranding the group.
TEST_P(RelationalDBTest, repackUpdateSuccessReplayDoesNotDemoteAlreadyReadyForDeletionSibling) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPLAY";
  const std::string tapePool = "repackTapePoolReplay";
  auto mountPolicy = makeMountPolicy(1000);
  cta::SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                           "/repack/buffer",
                                                           cta::common::dataStructures::RepackInfo::Type::MoveOnly,
                                                           mountPolicy,
                                                           false,
                                                           10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  const uint64_t repackRequestId = getRepackRequestId(db.getRelationalDB(), vid);

  constexpr uint64_t archiveFileId = 995004;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackArchivePendingJob(conn, repackRequestId, archiveFileId, tapePool, 1000, 0, 5, 0, 5, 1, 2);
    insertRepackArchivePendingJob(conn, repackRequestId, archiveFileId, tapePool, 1000, 0, 5, 0, 5, 2, 2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPLAY-DST");
  auto jobs = mount->getNextJobBatch(2, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Copy A's bookkeeping write "fails" (recovered via failReportBatch()'s repack branch, which
  // always resolves through updateRepackJobSuccess() - see that function's own comment). The
  // sibling hasn't settled yet, so A only parks.
  jobs.front()->prepareFailReport("simulated bookkeeping write recovery for copy A");
  std::vector<cta::SchedulerDatabase::ArchiveJob*> batchA {jobs.front().get()};
  mount->failReportBatch(batchA, lc);

  // Copy B settles the same way - the group is now complete, so this call promotes both A and B
  // to ReadyForDeletion together.
  jobs.back()->prepareFailReport("simulated bookkeeping write recovery for copy B");
  std::vector<cta::SchedulerDatabase::ArchiveJob*> batchB {jobs.back().get()};
  mount->failReportBatch(batchB, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                                "WHERE ARCHIVE_FILE_ID = :A AND STATUS = 'ReadyForDeletion'");
    stmt.bindUint64(":A", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("C"));
  }

  // Replay: copy A's recovery call fires again - jobs.front() is still a live job object
  // (failReportBatch() does not recycle it, unlike setJobBatchTransferred()/
  // setRepackJobBatchTransferred()), simulating a mount session retrying a write it believes
  // never completed.
  mount->failReportBatch(batchA, lc);

  // Both rows must still be exactly where the first settlement left them.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                                "WHERE ARCHIVE_FILE_ID = :A AND STATUS = 'ReadyForDeletion'");
    stmt.bindUint64(":A", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("C"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                                "WHERE ARCHIVE_FILE_ID = :A AND STATUS = 'AJS_ToReportToRepackForSuccess'");
    stmt.bindUint64(":A", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }
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

  // Simulate the physical transfer failing exactly as MigrationReportPacker does: accumulate via
  // prepareFailTransfer() (in-memory only), then flush through ArchiveMount::failTransferBatch().
  jobs.front()->prepareFailTransfer("simulated transfer failure");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobs.front().get()};
    mount->failTransferBatch(failBatch, lc);
  }

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

  // Simulate the physical transfer failing exactly as MigrationReportPacker does: accumulate via
  // prepareFailTransfer() (in-memory only), then flush through ArchiveMount::failTransferBatch().
  jobs.front()->prepareFailTransfer("simulated terminal transfer failure");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobs.front().get()};
    mount->failTransferBatch(failBatch, lc);
  }

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

//------------------------------------------------------------------------------
// Repack archive batch failure paths (the isRepack = true branches of
// failTransferBatchExceedRetries() / failTransferBatchRequeue()) and the
// transform's REQUEST_JOB_COUNT contract that every one of them depends on.
//------------------------------------------------------------------------------

// Counts rows of one file in REPACK_ARCHIVE_ACTIVE_QUEUE that are in the given status.
uint64_t
countRepackArchiveActive(cta::rdbms::Conn& conn, uint64_t archiveFileId, cta::schedulerdb::ArchiveJobStatus status) {
  auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_ACTIVE_QUEUE"
                              " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND STATUS = :STATUS");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.bindString(":STATUS", cta::to_string(status));
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw cta::exception::Exception("In countRepackArchiveActive(): no row returned.");
  }
  return rset.columnUint64("C");
}

uint64_t countRowsForFile(cta::rdbms::Conn& conn, const std::string& table, uint64_t archiveFileId) {
  auto stmt = conn.createStmt("SELECT COUNT(*) AS C FROM " + table + " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw cta::exception::Exception("In countRowsForFile(): no row returned.");
  }
  return rset.columnUint64("C");
}

// Inserts a finished repack retrieve job straight into REPACK_RETRIEVE_ACTIVE_QUEUE: driving one
// there through the scheduler would need a full tape session. retrievedCopyNb is the copy read off
// the repack tape - RepackRequest::addSubrequestsAndUpdateStats() always resolves the active copy
// number from the copy that lives on the repack VID (bestVid is only ever repackInfo.vid), so for a
// Move repack this is also the "moved" copy. rearchiveCopyNbs / rearchiveTapePools are the
// comma-separated columns Scheduler::expandRepackRequest() fills from copyNbsToRearchive.
void insertRepackRetrieveJobReadyForTransform(cta::rdbms::Conn& conn,
                                              uint64_t repackRequestId,
                                              uint64_t archiveFileId,
                                              const std::string& vid,
                                              const std::string& storageClass,
                                              uint16_t retrievedCopyNb,
                                              const std::string& rearchiveCopyNbs,
                                              const std::string& rearchiveTapePools) {
  auto stmt = conn.createStmt(R"SQL(
    INSERT INTO REPACK_RETRIEVE_ACTIVE_QUEUE (
      REPACK_REQUEST_ID, RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
      CREATION_TIME, START_TIME, STORAGE_CLASS, SIZE_IN_BYTES, VID, TAPE_POOL, MOUNT_POLICY,
      PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, MOUNT_ID,
      REPACK_REARCHIVE_COPY_NBS, REPACK_REARCHIVE_TAPE_POOLS
    ) VALUES (
      :REPACK_REQUEST_ID, :RETRIEVE_REQUEST_ID, 1, :STATUS, :ARCHIVE_FILE_ID,
      500, :START_TIME, :STORAGE_CLASS, 1000, :VID, 'tapePoolA', 'mountPolicy',
      1, 3600, :COPY_NB, 42,
      :REARCHIVE_COPY_NBS, :REARCHIVE_TAPE_POOLS
    )
  )SQL");
  stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
  // No uniqueness constraint on RETRIEVE_REQUEST_ID here; reusing archiveFileId keeps it simple and
  // mirrors what the transform copies into ARCHIVE_REQUEST_ID for every copy of the file.
  stmt.bindUint64(":RETRIEVE_REQUEST_ID", archiveFileId);
  stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.bindUint64(":START_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()));
  stmt.bindString(":STORAGE_CLASS", storageClass);
  stmt.bindString(":VID", vid);
  stmt.bindUint16(":COPY_NB", retrievedCopyNb);
  stmt.bindString(":REARCHIVE_COPY_NBS", rearchiveCopyNbs);
  stmt.bindString(":REARCHIVE_TAPE_POOLS", rearchiveTapePools);
  stmt.executeNonQuery();
}

// Queues a repack request of the given type and returns its REPACK_REQUEST_ID. The repack queue
// tables carry a foreign key on REPACK_REQUEST_ID, so the request must exist before any repack job
// row can be inserted.
uint64_t queueRepackAndGetId(cta::RelationalDBTestWrapper& db,
                             cta::log::LogContext& lc,
                             const std::string& vid,
                             cta::common::dataStructures::RepackInfo::Type type,
                             const cta::common::dataStructures::MountPolicy& mountPolicy) {
  cta::SchedulerDatabase::QueueRepackRequest repackRequest(vid, "/repack/buffer", type, mountPolicy, false, 10);
  repackRequest.m_creationLog = {"user", "host", 1000};
  db.queueRepack(repackRequest, lc);
  return getRepackRequestId(db.getRelationalDB(), vid);
}

// The transform's central contract: it inserts exactly one archive job per entry in
// REPACK_REARCHIVE_COPY_NBS, so rows inserted == CARDINALITY(REPACK_REARCHIVE_COPY_NBS) ==
// REQUEST_JOB_COUNT. MoveOnly lists only the moved copy, which IS the retrieved copy, so
// BASE_INSERT fires once and ALTERNATE_INSERT not at all.
TEST_P(RelationalDBTest, repackTransformMoveOnlyQueuesExactlyTheMovedCopy) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-XFORM-MOVEONLY";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 991001;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackRetrieveJobReadyForTransform(conn,
                                             repackRequestId,
                                             archiveFileId,
                                             vid,
                                             s_storageClassName,
                                             /*retrievedCopyNb=*/1,
                                             /*rearchiveCopyNbs=*/"1",
                                             /*rearchiveTapePools=*/"tapePoolA");
  }

  db.getNextSuccessfulRetrieveRepackReportBatch(lc);

  auto conn = db.getRelationalDB().getConn();
  auto stmt = conn.createStmt("SELECT COPY_NB, REQUEST_JOB_COUNT FROM REPACK_ARCHIVE_PENDING_QUEUE"
                              " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID ORDER BY COPY_NB");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  ASSERT_TRUE(rset.next());
  ASSERT_EQ(1u, rset.columnUint64("COPY_NB"));
  ASSERT_EQ(1u, rset.columnUint64("REQUEST_JOB_COUNT"));
  // Exactly one row: REQUEST_JOB_COUNT must equal the number of archive jobs actually queued.
  ASSERT_FALSE(rset.next());
}

// AddCopiesOnly: Scheduler::expandRepackRequest()'s moved-copy block is guarded on
// (MoveAndAddCopies || MoveOnly), so copyNbsToRearchive holds only the MISSING copies and the
// retrieved copy must not be re-archived - it is already on tape and totalFilesToArchive was never
// incremented for it. Regression test: BASE_INSERT used to fire unconditionally, queueing a second
// archive job for the existing copy and leaving REQUEST_JOB_COUNT (1) one short of the two rows
// inserted. A REQUEST_JOB_COUNT below the real group size makes every multi-copy completeness check
// (updateRepackJobSuccess(), moveFailedRepackJobBatchToFailedQueueTable()) pass on the first copy to
// settle, releasing the shared repack buffer file while the other copy is still being written.
TEST_P(RelationalDBTest, repackTransformAddCopiesOnlyQueuesOnlyTheMissingCopy) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-XFORM-ADDONLY";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::AddCopiesOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 991002;
  {
    auto conn = db.getRelationalDB().getConn();
    // Retrieved copy 1 already exists on tape; only copy 2 is missing and has to be archived.
    insertRepackRetrieveJobReadyForTransform(conn,
                                             repackRequestId,
                                             archiveFileId,
                                             vid,
                                             s_storageClassName,
                                             /*retrievedCopyNb=*/1,
                                             /*rearchiveCopyNbs=*/"2",
                                             /*rearchiveTapePools=*/"tapePoolB");
  }

  db.getNextSuccessfulRetrieveRepackReportBatch(lc);

  auto conn = db.getRelationalDB().getConn();
  auto stmt = conn.createStmt("SELECT COPY_NB, REQUEST_JOB_COUNT, TAPE_POOL FROM REPACK_ARCHIVE_PENDING_QUEUE"
                              " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID ORDER BY COPY_NB");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  ASSERT_TRUE(rset.next());
  // Only the added copy, on its own archive route - never the retrieved copy 1.
  ASSERT_EQ(2u, rset.columnUint64("COPY_NB"));
  ASSERT_EQ("tapePoolB", rset.columnString("TAPE_POOL"));
  ASSERT_EQ(1u, rset.columnUint64("REQUEST_JOB_COUNT"));
  ASSERT_FALSE(rset.next());
}

// A repack copy that exhausts its retries must settle ALONE. Its sibling is still being written by
// another drive, and for repack - unlike the user workflow - "every copy of the group has settled"
// is what releases the shared repack buffer file, so settling that sibling early would delete the
// source from under a live reader.
TEST_P(RelationalDBTest, repackArchiveFailTransferBatchExceedRetriesLeavesMidTransferSiblingAlone) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCH-SIB";
  const std::string tapePool = "repackTapePoolBatchSib";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveAndAddCopies, mountPolicy);

  constexpr uint64_t archiveFileId = 992001;
  constexpr uint64_t sizeInBytes = 7000;
  {
    auto conn = db.getRelationalDB().getConn();
    // Two copies of one file in one repack request: same ARCHIVE_REQUEST_ID, REQUEST_JOB_COUNT = 2.
    for (uint16_t copyNb : {uint16_t(1), uint16_t(2)}) {
      insertRepackArchivePendingJob(conn,
                                    repackRequestId,
                                    archiveFileId,
                                    tapePool,
                                    sizeInBytes,
                                    /*totalRetries=*/0,
                                    /*maxTotalRetries=*/1,
                                    /*totalReportRetries=*/0,
                                    /*maxReportRetries=*/2,
                                    copyNb,
                                    /*requestJobCount=*/2);
    }
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BATCH-SIB-DEST");
  auto jobs = mount->getNextJobBatch(2, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Fail exactly one copy; the other stays mid-transfer on this mount.
  jobs.front()->prepareFailTransfer("simulated terminal transfer failure for one copy");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(1u,
              countRepackArchiveActive(conn,
                                       archiveFileId,
                                       cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForFailure));
    // The untouched sibling must still be transferring. Keying the UPDATE on ARCHIVE_REQUEST_ID
    // (correct for a USER multi-copy request) would have settled it here too.
    ASSERT_EQ(
      1u,
      countRepackArchiveActive(conn, archiveFileId, cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack));
  }

  // The group is incomplete, so the reporting routine must move nothing and credit no failure -
  // moving would trigger deleteDiskFiles() on the buffer the live copy is still reading.
  db.getNextFailedArchiveRepackReportBatch(lc);
  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_FAILED_QUEUE", archiveFileId));
    ASSERT_EQ(2u, countRowsForFile(conn, "REPACK_ARCHIVE_ACTIVE_QUEUE", archiveFileId));
  }
  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(0u, repackInfo.failedFilesToArchive);
}

// Once every copy of the group has settled the whole group moves together, successes included, and
// the request is credited one failure per copy - the accounting unit is the archive job, and
// totalFilesToArchive counted one per copy too.
TEST_P(RelationalDBTest, repackArchiveFailTransferBatchExceedRetriesMovesGroupOnceEveryCopySettles) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCH-GRP";
  const std::string tapePool = "repackTapePoolBatchGrp";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveAndAddCopies, mountPolicy);

  constexpr uint64_t archiveFileId = 992002;
  constexpr uint64_t sizeInBytes = 7000;
  {
    auto conn = db.getRelationalDB().getConn();
    for (uint16_t copyNb : {uint16_t(1), uint16_t(2)}) {
      insertRepackArchivePendingJob(conn,
                                    repackRequestId,
                                    archiveFileId,
                                    tapePool,
                                    sizeInBytes,
                                    0,
                                    1,
                                    0,
                                    2,
                                    copyNb,
                                    /*requestJobCount=*/2);
    }
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BATCH-GRP-DEST");
  auto jobs = mount->getNextJobBatch(2, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Both copies fail in one batch, as they would if the whole mount's task queue failed.
  std::vector<SchedulerDatabase::ArchiveJob*> batch;
  for (auto& job : jobs) {
    job->prepareFailTransfer("simulated terminal transfer failure");
    batch.push_back(job.get());
  }
  mount->failTransferBatch(batch, lc);

  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(2u,
              countRepackArchiveActive(conn,
                                       archiveFileId,
                                       cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  }

  db.getNextFailedArchiveRepackReportBatch(lc);
  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_ACTIVE_QUEUE", archiveFileId));
    ASSERT_EQ(2u, countRowsForFile(conn, "REPACK_ARCHIVE_FAILED_QUEUE", archiveFileId));
  }
  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(2u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(2 * sizeInBytes, repackInfo.failedBytesToArchive);
}

// Single-copy repack through the batch path: no siblings to wait for, so the row settles and is
// swept on the very next reporting pass.
TEST_P(RelationalDBTest, repackArchiveFailTransferBatchExceedRetriesSingleCopy) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCH-ONE";
  const std::string tapePool = "repackTapePoolBatchOne";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 992003;
  constexpr uint64_t sizeInBytes = 5000;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackArchivePendingJob(conn, repackRequestId, archiveFileId, tapePool, sizeInBytes, 0, 1, 0, 2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BATCH-ONE-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("simulated terminal transfer failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  db.getNextFailedArchiveRepackReportBatch(lc);
  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_ACTIVE_QUEUE", archiveFileId));
    ASSERT_EQ(1u, countRowsForFile(conn, "REPACK_ARCHIVE_FAILED_QUEUE", archiveFileId));
  }
  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(1u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(sizeInBytes, repackInfo.failedBytesToArchive);
}

// The repack requeue branch (failTransferBatchRequeue with isRepack = true): with retries left the
// row goes back to REPACK_ARCHIVE_PENDING_QUEUE, not to the user tables.
TEST_P(RelationalDBTest, repackArchiveFailTransferBatchRequeuesBeforeExceedingRetries) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCH-RQ";
  const std::string tapePool = "repackTapePoolBatchRq";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 992004;
  {
    auto conn = db.getRelationalDB().getConn();
    // Plenty of retries left, both total and within-mount: this failure must requeue.
    insertRepackArchivePendingJob(conn, repackRequestId, archiveFileId, tapePool, 4000, 0, 3, 0, 2);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BATCH-RQ-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("simulated transient transfer failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_ACTIVE_QUEUE", archiveFileId));
  auto stmt = conn.createStmt("SELECT STATUS, TOTAL_RETRIES, RETRIES_WITHIN_MOUNT, MOUNT_ID"
                              " FROM REPACK_ARCHIVE_PENDING_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  ASSERT_TRUE(rset.next());
  ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack), rset.columnString("STATUS"));
  ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
  // Within-mount budget not exhausted, so the job keeps its mount for another attempt.
  ASSERT_EQ(1u, rset.columnUint64("RETRIES_WITHIN_MOUNT"));
  ASSERT_TRUE(rset.columnOptionalUint64("MOUNT_ID").has_value());
  ASSERT_FALSE(rset.next());
}

// The same ARCHIVE_FILE_ID can be in two repack requests at once - two tapes each holding one of
// its copies, both being repacked. Those rows are not siblings: each request has its own buffer
// copy, its own completeness and its own statistics. Grouping on bare ARCHIVE_FILE_ID would count
// request B's failed copy into request A's group, declare A complete, and sweep B's rows away with
// it (and delete B's buffer) off the back of an unrelated repack's failure.
TEST_P(RelationalDBTest, repackArchiveFailedGroupIsScopedToItsOwnRepackRequest) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  auto mountPolicy = makeMountPolicy(1000);
  const std::string vidA = "V-SCOPE-A";
  const std::string vidB = "V-SCOPE-B";
  const std::string tapePoolA = "repackScopePoolA";
  const std::string tapePoolB = "repackScopePoolB";
  const std::string tapePoolC = "repackScopePoolC";
  const uint64_t repackRequestIdA =
    queueRepackAndGetId(db, lc, vidA, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);
  const uint64_t repackRequestIdB =
    queueRepackAndGetId(db, lc, vidB, common::dataStructures::RepackInfo::Type::MoveAndAddCopies, mountPolicy);

  constexpr uint64_t archiveFileId = 993001;
  constexpr uint64_t sizeInBytes = 6000;
  {
    auto conn = db.getRelationalDB().getConn();
    // Request A: single copy of the file, complete on its own once it fails.
    insertRepackArchivePendingJob(conn, repackRequestIdA, archiveFileId, tapePoolA, sizeInBytes, 0, 1, 0, 2, 1, 1);
    // Request B: two copies of the SAME file. One will fail; the other is never dispatched, so B's
    // group stays incomplete and must not move.
    insertRepackArchivePendingJob(conn, repackRequestIdB, archiveFileId, tapePoolB, sizeInBytes, 0, 1, 0, 2, 1, 2);
    insertRepackArchivePendingJob(conn, repackRequestIdB, archiveFileId, tapePoolC, sizeInBytes, 0, 1, 0, 2, 2, 2);
  }

  for (const auto& [pool, dest] : std::vector<std::pair<std::string, std::string>> {
         {tapePoolA, "V-SCOPE-A-DEST"},
         {tapePoolB, "V-SCOPE-B-DEST"}
  }) {
    auto mount = createRepackArchiveMount(db, lc, pool, dest);
    auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
    ASSERT_EQ(1u, jobs.size());
    jobs.front()->prepareFailTransfer("simulated terminal transfer failure");
    std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
    mount->failTransferBatch(batch, lc);
  }

  db.getNextFailedArchiveRepackReportBatch(lc);

  {
    auto conn = db.getRelationalDB().getConn();
    // Only request A's copy moved.
    auto stmt = conn.createStmt("SELECT REPACK_REQUEST_ID FROM REPACK_ARCHIVE_FAILED_QUEUE"
                                " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(repackRequestIdA, rset.columnUint64("REPACK_REQUEST_ID"));
    ASSERT_FALSE(rset.next());
    // B's failed copy is still waiting for its sibling.
    ASSERT_EQ(1u,
              countRepackArchiveActive(conn,
                                       archiveFileId,
                                       cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  }

  ASSERT_EQ(1u, db.getRepackInfo(vidA).failedFilesToArchive);
  ASSERT_EQ(0u, db.getRepackInfo(vidB).failedFilesToArchive);
}

// A swept group is not uniformly a failure: when one copy of a multi-copy file fails permanently
// while its sibling reached tape and the catalogue, the sweep takes both rows - and each must be
// credited to the counter that matches its own outcome. Crediting the surviving copy as a failure
// keeps the totals balanced and still ends the request RRS_Failed, but reports a copy that IS on
// tape as lost, so `cta-admin repack ls` under-reports archived files.
TEST_P(RelationalDBTest, repackArchiveFailedGroupCreditsEachCopyByItsOwnOutcome) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-MIXED";
  const std::string tapePoolFailed = "repackMixedPoolFailed";
  const std::string tapePoolOk = "repackMixedPoolOk";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveAndAddCopies, mountPolicy);

  constexpr uint64_t archiveFileId = 996001;
  constexpr uint64_t sizeInBytes = 8000;
  {
    auto conn = db.getRelationalDB().getConn();
    // Two copies of one file, one per destination pool, so each can be driven independently.
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePoolFailed,
                                  sizeInBytes,
                                  /*totalRetries=*/0,
                                  /*maxTotalRetries=*/1,
                                  /*totalReportRetries=*/0,
                                  /*maxReportRetries=*/1,
                                  /*copyNb=*/1,
                                  /*requestJobCount=*/2);
    insertRepackArchivePendingJob(conn,
                                  repackRequestId,
                                  archiveFileId,
                                  tapePoolOk,
                                  sizeInBytes,
                                  0,
                                  1,
                                  0,
                                  1,
                                  /*copyNb=*/2,
                                  /*requestJobCount=*/2);
  }

  // Copy 1: transfer fails terminally.
  {
    auto mount = createRepackArchiveMount(db, lc, tapePoolFailed, "V-REPACK-MIXED-DEST1");
    auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
    ASSERT_EQ(1u, jobs.size());
    jobs.front()->prepareFailTransfer("simulated terminal transfer failure for copy 1");
    std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
    mount->failTransferBatch(batch, lc);
  }

  // Copy 2: reaches tape. Driving a real repack transfer success all the way through
  // reportJobsBatchTransferred() would need a full tape session, so take failReportBatch()'s repack
  // branch instead - for repack that branch IS updateRepackJobSuccess(), i.e. "recover the lost
  // bookkeeping write", which is exactly the success transition (see
  // ArchiveMount::failReportBatch()). The group is not all-success, so this copy settles at
  // AJS_ToReportToRepackForSuccess rather than being promoted to ReadyForDeletion.
  {
    auto mount = createRepackArchiveMount(db, lc, tapePoolOk, "V-REPACK-MIXED-DEST2");
    auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
    ASSERT_EQ(1u, jobs.size());
    jobs.front()->prepareFailReport("simulated scheduler bookkeeping write failure for copy 2");
    std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
    mount->failReportBatch(batch, lc);
  }

  {
    auto conn = db.getRelationalDB().getConn();
    ASSERT_EQ(1u,
              countRepackArchiveActive(conn,
                                       archiveFileId,
                                       cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForFailure));
    ASSERT_EQ(1u,
              countRepackArchiveActive(conn,
                                       archiveFileId,
                                       cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  }

  db.getNextFailedArchiveRepackReportBatch(lc);

  {
    auto conn = db.getRelationalDB().getConn();
    // Every copy has settled, so the whole group moves - the surviving copy's row included, which is
    // what makes the partial outcome inspectable.
    ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_ACTIVE_QUEUE", archiveFileId));
    ASSERT_EQ(2u, countRowsForFile(conn, "REPACK_ARCHIVE_FAILED_QUEUE", archiveFileId));
  }

  const auto repackInfo = db.getRepackInfo(vid);
  // One copy lost, one copy on tape - and the counters say so.
  ASSERT_EQ(1u, repackInfo.failedFilesToArchive);
  ASSERT_EQ(sizeInBytes, repackInfo.failedBytesToArchive);
  ASSERT_EQ(1u, repackInfo.archivedFiles);
  ASSERT_EQ(sizeInBytes, repackInfo.archivedBytes);
  // Totals still balance against totalFilesToArchive, so the request can still complete, and it
  // still ends RRS_Failed because a failure was recorded at all - that, not the failure count, is
  // what keeps the source tape from being reclaimed.
  ASSERT_EQ(repackInfo.failedFilesToArchive + repackInfo.archivedFiles, repackInfo.totalFilesToArchive);
}

// Repack report failure through the batch path for a single-copy request: "report retries
// exhausted" for repack means the bookkeeping WRITE failed, not that the job failed, so
// failReportBatchExceedRetries() delegates to updateRepackJobSuccess() and the row must end up
// ReadyForDeletion (a recovered success), never in the failed queue.
TEST_P(RelationalDBTest, repackArchiveFailReportBatchSingleCopyRecoversAsSuccess) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCH-RPT";
  const std::string tapePool = "repackTapePoolBatchRpt";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 994001;
  {
    auto conn = db.getRelationalDB().getConn();
    insertRepackArchivePendingJob(conn, repackRequestId, archiveFileId, tapePool, 3000, 0, 2, 0, 1);
  }

  auto mount = createRepackArchiveMount(db, lc, tapePool, "V-REPACK-BATCH-RPT-DEST");
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailReport("simulated scheduler bookkeeping write failure");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  ASSERT_EQ(0u, countRowsForFile(conn, "REPACK_ARCHIVE_FAILED_QUEUE", archiveFileId));
  ASSERT_EQ(1u, countRepackArchiveActive(conn, archiveFileId, cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion));
}

// Covers: repack transfer and catalogue update both succeed, but the write recording that in
// the scheduler DB fails - failReportBatch() is only reachable once the catalogue write already
// committed (ArchiveMount::reportJobsBatchTransferred()'s catalogue_updated gate). For repack,
// failReportBatchExceedRetries() recovers the lost write via updateRepackJobSuccess() (row ends
// up ReadyForDeletion) rather than mislabelling a success as a failure.
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

  // Simulate the scheduler-DB bookkeeping write failing after a successful transfer exactly as
  // MigrationReportPacker's FailedReportMoveToQueue catch does: accumulate via prepareFailReport()
  // (in-memory only), then flush through ArchiveMount::failReportBatch().
  jobs.front()->prepareFailReport("simulated scheduler bookkeeping write failure");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobs.front().get()};
    mount->failReportBatch(failBatch, lc);
  }

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
  jobsA.front()->prepareFailReport("simulated scheduler bookkeeping write failure for copy 1");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobsA.front().get()};
    mountA->failReportBatch(failBatch, lc);
  }

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
  jobsB.front()->prepareFailReport("simulated scheduler bookkeeping write failure for copy 2");
  {
    std::vector<cta::SchedulerDatabase::ArchiveJob*> failBatch {jobsB.front().get()};
    mountB->failReportBatch(failBatch, lc);
  }

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

// Regression test for updateRepackJobSuccess() settling on bare ARCHIVE_FILE_ID with no STATUS
// guard. Unlike the test above, where copy 2 still sits in the PENDING table when copy 1 settles,
// this dispatches BOTH copies into REPACK_ARCHIVE_ACTIVE_QUEUE first - before the fix, copy 1's
// settlement then marked the still-transferring copy 2 AJS_ToReportToRepackForSuccess purely for
// sharing its ARCHIVE_FILE_ID.
TEST_P(RelationalDBTest, repackArchiveBookkeepingReportFailureDoesNotTouchInFlightSibling) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-INFLIGHT";
  const std::string tapePoolA = "repackTapePoolInFlightA";
  const std::string tapePoolB = "repackTapePoolInFlightB";
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

  constexpr uint64_t archiveFileId = 990005;
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

  // Both copies dispatched into REPACK_ARCHIVE_ACTIVE_QUEUE before either settles - copy 2 is
  // genuinely in flight, not merely queued, when copy 1's success is processed below.
  auto mountA = createRepackArchiveMount(db, lc, tapePoolA, "V-INFLIGHT-DESTA");
  auto jobsA = mountA->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  auto mountB = createRepackArchiveMount(db, lc, tapePoolB, "V-INFLIGHT-DESTB");
  auto jobsB = mountB->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());

  // Copy 1 transfers and its catalogue update succeeds, but the scheduler-DB bookkeeping write
  // fails - recovered the same way as the test above.
  jobsA.front()->failReport("simulated scheduler bookkeeping write failure for copy 1", lc);

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(
      "SELECT COPY_NB, STATUS FROM REPACK_ARCHIVE_ACTIVE_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "ORDER BY COPY_NB");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();

    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToReportToRepackForSuccess),
              rset.columnString("STATUS"));

    // Copy 2 must be untouched: still actively transferring, not jumped to success just because
    // it shares copy 1's ARCHIVE_FILE_ID.
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(cta::to_string(cta::schedulerdb::ArchiveJobStatus::AJS_ToTransferForRepack), rset.columnString("STATUS"));

    ASSERT_FALSE(rset.next());
  }
  {
    // Nothing credited or deleted yet - the request is still incomplete.
    const auto repackInfoMidway = db.getRepackInfo(vid);
    ASSERT_EQ(0u, repackInfoMidway.archivedFiles);
    ASSERT_EQ(0u, repackInfoMidway.archivedBytes);
  }

  // Now copy 2 also transfers and settles the same way - this is the last copy, so the recovery
  // must complete the whole request.
  jobsB.front()->failReport("simulated scheduler bookkeeping write failure for copy 2", lc);

  {
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

  db.getNextSuccessfulArchiveRepackReportBatch(lc);

  const auto repackInfo = db.getRepackInfo(vid);
  ASSERT_EQ(2u, repackInfo.archivedFiles);
  ASSERT_EQ(2 * sizeInBytes, repackInfo.archivedBytes);
}

// Two-connection regression test for the lock order of updateRepackRequestsProgress(): a batch
// spanning several repack requests must lock their tracking rows in REPACK_REQUEST_ID order
// (ORDER BY before FOR NO KEY UPDATE), or two concurrent batches over overlapping requests can
// deadlock A-then-B against B-then-A. Connection 1 pre-locks the HIGHER id - where an ordered
// worker comes to wait - so once the worker blocks, a NOWAIT probe of the LOWER id must fail
// (the ordered worker already holds it). An unordered worker waiting on the higher id would hold
// nothing, the probe would succeed, and a deadlock-aborted batch would additionally fail the
// final counter checks.
TEST_P(RelationalDBTest, repackProgressBatchLocksTrackingRowsInRequestIdOrder) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  // Two repack requests; REPACK_REQUEST_ID is BIGSERIAL, so the first queued gets the lower id.
  auto mountPolicy = makeMountPolicy(1000);
  for (const std::string vid : {"V-LOCKORD-A", "V-LOCKORD-B"}) {
    SchedulerDatabase::QueueRepackRequest repackRequest(vid,
                                                        "/repack/buffer",
                                                        common::dataStructures::RepackInfo::Type::AddCopiesOnly,
                                                        mountPolicy,
                                                        false,
                                                        10);
    repackRequest.m_creationLog = {"user", "host", 1000};
    db.queueRepack(repackRequest, lc);
  }
  const uint64_t idA = getRepackRequestId(db.getRelationalDB(), "V-LOCKORD-A");
  const uint64_t idB = getRepackRequestId(db.getRelationalDB(), "V-LOCKORD-B");
  ASSERT_LT(idA, idB);

  // One finished retrieve row per request, so a single progress batch spans both.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt(R"SQL(
      INSERT INTO REPACK_RETRIEVE_ACTIVE_QUEUE (
        REPACK_REQUEST_ID, RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
        CREATION_TIME, START_TIME, STORAGE_CLASS, SIZE_IN_BYTES, VID, TAPE_POOL, MOUNT_POLICY,
        PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, MOUNT_ID, REPACK_REARCHIVE_COPY_NBS,
        REPACK_REARCHIVE_TAPE_POOLS
      ) VALUES
      (:REPACK_REQUEST_ID_A, 883, 1, :STATUS_A, 993, 500, 500, :STORAGE_CLASS_A, 1000, :VID_A, 'tapePoolA',
       'mountPolicy', 1, 3600, 1, 42, '2', 'tapePoolB'),
      (:REPACK_REQUEST_ID_B, 884, 1, :STATUS_B, 994, 500, 500, :STORAGE_CLASS_B, 1000, :VID_B, 'tapePoolA',
       'mountPolicy', 1, 3600, 1, 43, '2', 'tapePoolB')
    )SQL");
    stmt.bindUint64(":REPACK_REQUEST_ID_A", idA);
    stmt.bindUint64(":REPACK_REQUEST_ID_B", idB);
    stmt.bindString(":STATUS_A", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindString(":STATUS_B", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForSuccess));
    stmt.bindString(":STORAGE_CLASS_A", s_storageClassName);
    stmt.bindString(":STORAGE_CLASS_B", s_storageClassName);
    stmt.bindString(":VID_A", "V-LOCKORD-A");
    stmt.bindString(":VID_B", "V-LOCKORD-B");
    stmt.executeNonQuery();
  }

  // Connection 1 locks the higher id. Its backend pid identifies it as the blocker below.
  auto conn1 = db.getRelationalDB().getConn();
  conn1.executeNonQuery("BEGIN");
  uint64_t conn1Pid = 0;
  {
    auto stmt = conn1.createStmt("SELECT PG_BACKEND_PID() AS PID");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    conn1Pid = rset.columnUint64("PID");
  }
  {
    auto stmt = conn1.createStmt(
      "SELECT REPACK_REQUEST_ID FROM REPACK_REQUEST_TRACKING WHERE REPACK_REQUEST_ID = :ID FOR NO KEY UPDATE");
    stmt.bindUint64(":ID", idB);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
  }

  // The worker transforms both rows and updates both tracking rows in one transaction - the
  // production path through updateRepackRequestsProgress(). It must block on the higher id.
  // catch (...) so an unexpected exception fails this test via the counter checks below instead
  // of escaping the thread and std::terminate()ing the whole test binary.
  std::thread worker([&db] {
    try {
      auto workerLogger = makeLogger();
      cta::log::LogContext workerLc(*workerLogger);
      db.getNextSuccessfulRetrieveRepackReportBatch(workerLc);
    } catch (...) {}
  });

  // Wait until a session blocked BY conn1 sits in a row-lock wait (no gtest assertions while the
  // thread runs and locks are held - every exit path below must still rollback and join).
  // Polled from a separate autocommit connection: activity views are snapshotted per transaction,
  // so polling inside conn1's open transaction would keep re-reading the first poll's snapshot.
  // pg_blocking_pids() scopes the wait to OUR worker, not any unrelated session's.
  bool sawLockWait = false;
  {
    auto pollConn = db.getRelationalDB().getConn();
    for (int i = 0; i < 100 && !sawLockWait; i++) {
      auto stmt = pollConn.createStmt("SELECT COUNT(*) AS C FROM PG_STAT_ACTIVITY "
                                      "WHERE WAIT_EVENT_TYPE = 'Lock' "
                                      "AND CAST(:BLOCKER_PID AS INTEGER) = ANY(PG_BLOCKING_PIDS(PID))");
      stmt.bindUint64(":BLOCKER_PID", conn1Pid);
      auto rset = stmt.executeQuery();
      rset.next();
      sawLockWait = rset.columnUint64("C") > 0;
      if (!sawLockWait) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  }

  bool probeThrew = false;
  if (sawLockWait) {
    try {
      auto stmt = conn1.createStmt("SELECT REPACK_REQUEST_ID FROM REPACK_REQUEST_TRACKING "
                                   "WHERE REPACK_REQUEST_ID = :ID FOR NO KEY UPDATE NOWAIT");
      stmt.bindUint64(":ID", idA);
      auto rset = stmt.executeQuery();
      rset.next();
    } catch (...) {
      probeThrew = true;  // lock_not_available: the waiting worker already holds the lower id
    }
  }

  conn1.rollback();  // also clears the aborted-transaction state the failed NOWAIT probe left
  worker.join();

  ASSERT_TRUE(sawLockWait);
  EXPECT_TRUE(probeThrew) << "worker waited on the higher REPACK_REQUEST_ID without holding the lower one - "
                             "tracking-row locks are not taken in id order";

  // Both requests credited - this would also catch a deadlock-aborted batch.
  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("SELECT RETRIEVED_FILES FROM REPACK_REQUEST_TRACKING "
                                "WHERE REPACK_REQUEST_ID IN (:IDA, :IDB) ORDER BY REPACK_REQUEST_ID");
    stmt.bindUint64(":IDA", idA);
    stmt.bindUint64(":IDB", idB);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("RETRIEVED_FILES"));
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("RETRIEVED_FILES"));
    ASSERT_FALSE(rset.next());
  }
}

// Covers the BATCH equivalent of repackArchiveBookkeepingReportFailureRecoversMultiCopyCorrectly:
// ArchiveMount::failReportBatch() -> ArchiveJobQueueRow::failReportBatchExceedRetries(isRepack=true)
// must route repack jobs through updateRepackJobSuccess() exactly like the single-job
// ArchiveRdbJob::failReport() path, not the user-job move-to-ARCHIVE_FAILED_QUEUE path - even when
// both copies of a multi-copy request are bunched into the SAME batch call together. Before this
// fix, the batch path moved repack rows straight to REPACK_ARCHIVE_FAILED_QUEUE without deleting
// the originals from REPACK_ARCHIVE_ACTIVE_QUEUE, duplicating every row and leaving the active
// copy stuck there forever - this also masked the multi-copy premature-deletion risk, since the
// row never reached ReadyForDeletion via updateRepackJobSuccess() at all.
TEST_P(RelationalDBTest, repackArchiveBatchBookkeepingReportFailureRecoversMultiCopyCorrectly) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-BATCHMULTI";
  const std::string tapePoolA = "repackTapePoolBatchMultiA";
  const std::string tapePoolB = "repackTapePoolBatchMultiB";
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

  constexpr uint64_t archiveFileId = 990005;
  constexpr uint64_t sizeInBytes = 12000;
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

  auto mountA = createRepackArchiveMount(db, lc, tapePoolA, "V-BATCHMULTI-DESTA");
  auto jobsA = mountA->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  auto mountB = createRepackArchiveMount(db, lc, tapePoolB, "V-BATCHMULTI-DESTB");
  auto jobsB = mountB->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());

  // Both copies' bookkeeping write fails and both land in ONE batch call together - the actual
  // "bunching" this mechanism exists for, not two sequential single-job calls.
  jobsA.front()->prepareFailReport("simulated scheduler bookkeeping write failure for copy 1");
  jobsB.front()->prepareFailReport("simulated scheduler bookkeeping write failure for copy 2");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobsA.front().get(), jobsB.front().get()};
  mountA->failReportBatch(batch, lc);

  {
    // Both copies are ReadyForDeletion, still in REPACK_ARCHIVE_ACTIVE_QUEUE exactly once each -
    // not moved, not duplicated.
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
  {
    // Neither copy was duplicated into REPACK_ARCHIVE_FAILED_QUEUE.
    auto conn = db.getRelationalDB().getConn();
    auto stmt =
      conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_ARCHIVE_FAILED_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("C"));
  }

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

//------------------------------------------------------------------------------
// Repack retrieve batch failure paths (the isRepack = true branches of
// RetrieveJobQueueRow::failTransferBatchExceedRetries() / failTransferBatchRequeue()).
// Reached in production from RecallReportPacker::flushFailedJobs() -> RetrieveMount::
// failTransferBatch(): a repack retrieve is a real tape read and fails like any other.
//------------------------------------------------------------------------------

// Inserts a repack retrieve job ready to be dispatched to a mount. Every column
// RetrieveJobQueueRow::initialize() reads with a NoOpt accessor has to be non-NULL, hence the long
// list. ALTERNATE_* are single-entry lists naming this very copy: that is what the real insert
// produces for a one-copy file (RetrieveRequest::makeJobRow() appends every job, the active one
// included), and it keeps selectNextString()'s index valid for the rotation path.
void insertRepackRetrievePendingJob(cta::rdbms::Conn& conn,
                                    uint64_t repackRequestId,
                                    uint64_t archiveFileId,
                                    const std::string& vid,
                                    const std::string& tapePool,
                                    uint32_t totalRetries,
                                    uint32_t maxTotalRetries,
                                    uint32_t maxRetriesWithinMount) {
  auto stmt = conn.createStmt(R"SQL(
    INSERT INTO REPACK_RETRIEVE_PENDING_QUEUE (
      REPACK_REQUEST_ID, RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
      SIZE_IN_BYTES, CREATION_TIME, START_TIME, STORAGE_CLASS, VID, TAPE_POOL, MOUNT_POLICY,
      PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, FSEQ, BLOCK_ID,
      ALTERNATE_VIDS, ALTERNATE_COPY_NBS, ALTERNATE_FSEQS, ALTERNATE_BLOCK_IDS,
      RETRIEVE_REPORT_URL, RETRIEVE_ERROR_REPORT_URL, REQUESTER_NAME, REQUESTER_GROUP, DST_URL,
      RETRIES_WITHIN_MOUNT, TOTAL_RETRIES, LAST_MOUNT_WITH_FAILURE,
      MAX_TOTAL_RETRIES, MAX_RETRIES_WITHIN_MOUNT, TOTAL_REPORT_RETRIES, MAX_REPORT_RETRIES,
      DRIVE, HOST, LOGICAL_LIBRARY,
      REPACK_REARCHIVE_COPY_NBS, REPACK_REARCHIVE_TAPE_POOLS
    ) VALUES (
      :REPACK_REQUEST_ID, :RETRIEVE_REQUEST_ID, 1, :STATUS, :ARCHIVE_FILE_ID,
      1000, 500, :START_TIME, :STORAGE_CLASS, :VID, :TAPE_POOL, 'mountPolicy',
      1, 0, 1, 10, 100,
      :VID, '1', '10', '100',
      'test://retrieve-report-url', 'test://retrieve-error-report-url', 'user', 'group',
      'root://disk/repack-retrieve',
      0, :TOTAL_RETRIES, 0,
      :MAX_TOTAL_RETRIES, :MAX_RETRIES_WITHIN_MOUNT, 0, 2,
      'drive', 'host', 'loglib',
      '1,2', :REARCHIVE_TAPE_POOLS
    )
  )SQL");
  stmt.bindUint64(":REPACK_REQUEST_ID", repackRequestId);
  stmt.bindUint64(":RETRIEVE_REQUEST_ID", archiveFileId);
  stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToTransfer));
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.bindUint64(":START_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()));
  stmt.bindString(":STORAGE_CLASS", s_storageClassName);
  stmt.bindString(":VID", vid);
  stmt.bindString(":TAPE_POOL", tapePool);
  stmt.bindUint32(":TOTAL_RETRIES", totalRetries);
  stmt.bindUint32(":MAX_TOTAL_RETRIES", maxTotalRetries);
  stmt.bindUint32(":MAX_RETRIES_WITHIN_MOUNT", maxRetriesWithinMount);
  stmt.bindString(":REARCHIVE_TAPE_POOLS", tapePool + "," + tapePool);
  stmt.executeNonQuery();
}

// Inserts a plain (non-repack) user retrieve job ready to be dispatched to a mount, with TWO real
// alternate copies rather than the single self-referencing entry queueRetrieveJob() produces.
// Lets a test drive RetrieveRdbJob::prepareFailTransfer()'s alternate-VID rotation branch
// (within-mount retries exhausted but not terminal) against a genuine second copy, rather than
// the no-op "rotate to itself" that every other test in this file exercises.
void insertRetrievePendingJobWithAlternates(cta::rdbms::Conn& conn,
                                            uint64_t archiveFileId,
                                            const std::string& vid,
                                            const std::string& tapePool,
                                            const std::string& alternateVids,
                                            const std::string& alternateCopyNbs,
                                            const std::string& alternateFSeqs,
                                            const std::string& alternateBlockIds,
                                            uint8_t copyNb,
                                            uint64_t fSeq,
                                            uint64_t blockId,
                                            uint32_t maxTotalRetries,
                                            uint32_t maxRetriesWithinMount) {
  auto stmt = conn.createStmt(R"SQL(
    INSERT INTO RETRIEVE_PENDING_QUEUE (
      RETRIEVE_REQUEST_ID, REQUEST_JOB_COUNT, STATUS, ARCHIVE_FILE_ID,
      SIZE_IN_BYTES, CREATION_TIME, START_TIME, STORAGE_CLASS, VID, TAPE_POOL, MOUNT_POLICY,
      PRIORITY, MIN_RETRIEVE_REQUEST_AGE, COPY_NB, FSEQ, BLOCK_ID,
      ALTERNATE_VIDS, ALTERNATE_COPY_NBS, ALTERNATE_FSEQS, ALTERNATE_BLOCK_IDS,
      RETRIEVE_REPORT_URL, RETRIEVE_ERROR_REPORT_URL, REQUESTER_NAME, REQUESTER_GROUP, DST_URL,
      RETRIES_WITHIN_MOUNT, TOTAL_RETRIES, LAST_MOUNT_WITH_FAILURE,
      MAX_TOTAL_RETRIES, MAX_RETRIES_WITHIN_MOUNT, TOTAL_REPORT_RETRIES, MAX_REPORT_RETRIES,
      DRIVE, HOST, LOGICAL_LIBRARY
    ) VALUES (
      :ARCHIVE_FILE_ID, 1, :STATUS, :ARCHIVE_FILE_ID,
      1000, 500, :START_TIME, :STORAGE_CLASS, :VID, :TAPE_POOL, 'mountPolicy',
      1, 0, :COPY_NB, :FSEQ, :BLOCK_ID,
      :ALT_VIDS, :ALT_COPY_NBS, :ALT_FSEQS, :ALT_BLOCK_IDS,
      'test://retrieve-report-url', 'test://retrieve-error-report-url', 'user', 'group',
      'root://disk/alt-rotation',
      0, 0, 0,
      :MAX_TOTAL_RETRIES, :MAX_RETRIES_WITHIN_MOUNT, 0, 2,
      'drive', 'host', 'loglib'
    )
  )SQL");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.bindString(":STATUS", cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToTransfer));
  stmt.bindUint64(":START_TIME", static_cast<uint64_t>(cta::utils::getCurrentEpochTime()));
  stmt.bindString(":STORAGE_CLASS", s_storageClassName);
  stmt.bindString(":VID", vid);
  stmt.bindString(":TAPE_POOL", tapePool);
  stmt.bindUint8(":COPY_NB", copyNb);
  stmt.bindUint64(":FSEQ", fSeq);
  stmt.bindUint64(":BLOCK_ID", blockId);
  stmt.bindString(":ALT_VIDS", alternateVids);
  stmt.bindString(":ALT_COPY_NBS", alternateCopyNbs);
  stmt.bindString(":ALT_FSEQS", alternateFSeqs);
  stmt.bindString(":ALT_BLOCK_IDS", alternateBlockIds);
  stmt.bindUint32(":MAX_TOTAL_RETRIES", maxTotalRetries);
  stmt.bindUint32(":MAX_RETRIES_WITHIN_MOUNT", maxRetriesWithinMount);
  stmt.executeNonQuery();
}

// Creates a real repack retrieve mount on vid, exactly as a tape server would, so the jobs it
// returns carry isRepack the same way production does.
std::unique_ptr<cta::SchedulerDatabase::RetrieveMount>
createRepackRetrieveMount(cta::RelationalDBTestWrapper& db, cta::log::LogContext& lc, const std::string& vid) {
  using namespace cta;
  auto mountInfo = db.getMountInfo(lc);
  const auto repackVo = db.getRelationalDB().getDefaultRepackVo();
  if (!repackVo) {
    throw cta::exception::Exception("In createRepackRetrieveMount(): no default repack VO configured");
  }
  for (auto& pm : mountInfo->potentialMounts) {
    if (pm.type == common::dataStructures::MountType::Retrieve && pm.vid == vid) {
      // The scheduler normally supplies the VO before creating the mount.
      pm.vo = repackVo->name;
      return mountInfo->createRetrieveMount(pm, "drive", "library", "host");
    }
  }
  throw cta::exception::Exception("In createRepackRetrieveMount(): no Retrieve potential mount for VID " + vid);
}

// Repack retrieve, retries exhausted: the row must be flagged for repack failure reporting in
// REPACK_RETRIEVE_ACTIVE_QUEUE - the REPACK_ prefixed tables, not the user ones.
TEST_P(RelationalDBTest, repackRetrieveFailTransferBatchExceedRetries) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-RETR-EXC";
  const std::string tapePool = "repackRetrPoolExc";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveOnly, mountPolicy);

  constexpr uint64_t archiveFileId = 995001;
  {
    auto conn = db.getRelationalDB().getConn();
    // One retry away from terminal: prepareFailTransfer() takes totalRetries to 1 = maxTotalRetries.
    insertRepackRetrievePendingJob(conn, repackRequestId, archiveFileId, vid, tapePool, 0, 1, 1);
  }

  auto mount = createRepackRetrieveMount(db, lc, vid);
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("simulated terminal repack retrieve failure");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  auto stmt = conn.createStmt("SELECT STATUS, TOTAL_RETRIES FROM REPACK_RETRIEVE_ACTIVE_QUEUE"
                              " WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  ASSERT_TRUE(rset.next());
  ASSERT_EQ(cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToReportToRepackForFailure),
            rset.columnString("STATUS"));
  ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
  ASSERT_FALSE(rset.next());
  // The user tables must be untouched.
  {
    auto userStmt = conn.createStmt("SELECT COUNT(*) AS C FROM RETRIEVE_ACTIVE_QUEUE");
    auto userRset = userStmt.executeQuery();
    ASSERT_TRUE(userRset.next());
    ASSERT_EQ(0u, userRset.columnUint64("C"));
  }
}

// Repack retrieve, retries remaining: the row goes back to REPACK_RETRIEVE_PENDING_QUEUE, and the
// REPACK_REARCHIVE_* columns - which only the repack branch of the requeue INSERT carries, and
// which the later retrieve-to-archive transform depends on for both its archive routes and its
// REQUEST_JOB_COUNT - must survive the round trip.
TEST_P(RelationalDBTest, repackRetrieveFailTransferBatchRequeuePreservesRearchiveColumns) {
  using namespace cta;

  auto logger = makeLogger();
  cta::log::LogContext lc(*logger);
  auto& db = getDb();

  const std::string vid = "V-REPACK-RETR-RQ";
  const std::string tapePool = "repackRetrPoolRq";
  auto mountPolicy = makeMountPolicy(1000);
  const uint64_t repackRequestId =
    queueRepackAndGetId(db, lc, vid, common::dataStructures::RepackInfo::Type::MoveAndAddCopies, mountPolicy);

  constexpr uint64_t archiveFileId = 995002;
  {
    auto conn = db.getRelationalDB().getConn();
    // Plenty of budget left, within-mount included: this failure must requeue and keep its mount.
    insertRepackRetrievePendingJob(conn, repackRequestId, archiveFileId, vid, tapePool, 0, 3, 3);
  }

  auto mount = createRepackRetrieveMount(db, lc, vid);
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("simulated transient repack retrieve failure");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto activeStmt = conn.createStmt("SELECT COUNT(*) AS C FROM REPACK_RETRIEVE_ACTIVE_QUEUE");
    auto activeRset = activeStmt.executeQuery();
    ASSERT_TRUE(activeRset.next());
    ASSERT_EQ(0u, activeRset.columnUint64("C"));
  }
  auto stmt = conn.createStmt("SELECT STATUS, TOTAL_RETRIES, RETRIES_WITHIN_MOUNT, REPACK_REQUEST_ID,"
                              " REPACK_REARCHIVE_COPY_NBS, REPACK_REARCHIVE_TAPE_POOLS"
                              " FROM REPACK_RETRIEVE_PENDING_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  ASSERT_TRUE(rset.next());
  ASSERT_EQ(cta::to_string(cta::schedulerdb::RetrieveJobStatus::RJS_ToTransfer), rset.columnString("STATUS"));
  ASSERT_EQ(1u, rset.columnUint64("TOTAL_RETRIES"));
  ASSERT_EQ(1u, rset.columnUint64("RETRIES_WITHIN_MOUNT"));
  ASSERT_EQ(repackRequestId, rset.columnUint64("REPACK_REQUEST_ID"));
  ASSERT_EQ("1,2", rset.columnString("REPACK_REARCHIVE_COPY_NBS"));
  ASSERT_EQ(tapePool + "," + tapePool, rset.columnString("REPACK_REARCHIVE_TAPE_POOLS"));
  ASSERT_FALSE(rset.next());
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM ARCHIVE_PENDING_QUEUE"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM RETRIEVE_PENDING_QUEUE"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT, MAX(TOTAL_RETRIES) AS MAX_RETRIES FROM ARCHIVE_PENDING_QUEUE"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
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
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_RETRIES_WITHIN_MOUNT = 1");
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
  // Job requeued for a different mount: MOUNT_ID must be NULL, and RETRIES_WITHIN_MOUNT must be
  // reset to 0 rather than written through at its exhausted value - exactly what the single-job
  // ArchiveRdbJob::requeueJobToMount() does. Leaving it at the ceiling would deny the job its
  // within-mount budget if this same still-running mount re-popped the row (MOUNT_ID is NULL, so it
  // can): updateRetryCounts() only resets the counter when it sees a different mount id.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
                                " WHERE STATUS = 'AJS_ToTransferForUser' AND MOUNT_ID IS NULL"
                                " AND RETRIES_WITHIN_MOUNT = 0");
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
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_RETRIES_WITHIN_MOUNT = 1");
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_PENDING_QUEUE"
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

// RetrieveRdbJob::prepareFailTransfer()'s alternate-VID rotation (scheduler/rdbms/RetrieveRdbJob.cpp)
// parses ALTERNATE_VIDS/ALTERNATE_COPY_NBS/ALTERNATE_FSEQS/ALTERNATE_BLOCK_IDS as parallel
// comma-separated lists and indexes into them with whatever position selectNextString() picks.
// retrieveFailTransferBatchRequeue_withinMountExhausted above only ever has a single,
// self-referencing alternate (index 0 = itself, a no-op), so an off-by-one or a mismatched
// ordering between the four parallel lists would go uncaught. This test gives the job a genuine
// second copy so the rotation actually has to land on a different entry.
TEST_P(RelationalDBTest, retrieveFailTransferBatchRequeue_rotatesToSecondAlternate) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  constexpr uint64_t archiveFileId = 336;
  const std::string vid1 = "V-ALT-ROT-1";
  const std::string vid2 = "V-ALT-ROT-2";
  const std::string tapePool = "altRotationPool";

  {
    auto conn = db.getRelationalDB().getConn();
    // Current copy is copy 1 on vid1 (fSeq=10, blockId=100); the real second alternate is copy 2
    // on vid2 (fSeq=20, blockId=200). selectNextString() finds vid1 at index 0 in ALTERNATE_VIDS
    // and returns the entry at index 1 - vid2 - so a correct rotation must pick up copy 2's
    // fields, not copy 1's again.
    insertRetrievePendingJobWithAlternates(conn,
                                           archiveFileId,
                                           vid1,
                                           tapePool,
                                           vid1 + "," + vid2,
                                           "1,2",
                                           "10,20",
                                           "100,200",
                                           /*copyNb=*/1,
                                           /*fSeq=*/10,
                                           /*blockId=*/100,
                                           /*maxTotalRetries=*/2,
                                           /*maxRetriesWithinMount=*/1);
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());
  auto mount = mountInfo->createRetrieveMount(mountInfo->potentialMounts.front(), "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  // maxRetriesWithinMount=1: updateRetryCounts() sets retriesWithinMount to 1 on this first
  // failure (lastMountWithFailure starts at 0, differs from the real mount id), which already
  // meets the threshold - so prepareFailTransfer() takes the rotation branch immediately.
  jobs.front()->prepareFailTransfer("within-mount exhausted, rotate to second alternate");
  std::vector<SchedulerDatabase::RetrieveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // The requeued row must carry the SECOND alternate's fields (vid2/copy 2/fSeq 20/blockId 200),
    // not the first (which an off-by-one, or a rotation that silently no-ops, would instead leave
    // in place), and RETRIES_WITHIN_MOUNT must be reset to 0 for the new mount.
    auto stmt = conn.createStmt("SELECT VID, COPY_NB, FSEQ, BLOCK_ID, RETRIES_WITHIN_MOUNT, MOUNT_ID"
                                " FROM RETRIEVE_PENDING_QUEUE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID");
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(vid2, rset.columnString("VID"));
    ASSERT_EQ(2u, rset.columnUint64("COPY_NB"));
    ASSERT_EQ(20u, rset.columnUint64("FSEQ"));
    ASSERT_EQ(200u, rset.columnUint64("BLOCK_ID"));
    ASSERT_EQ(0u, rset.columnUint64("RETRIES_WITHIN_MOUNT"));
    ASSERT_FALSE(rset.columnOptionalUint64("MOUNT_ID").has_value());
    ASSERT_FALSE(rset.next());
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
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // A2, A3 are requeued for another attempt
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
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
    auto stmt = conn.createStmt("SELECT FAILURE_LOG FROM ARCHIVE_PENDING_QUEUE WHERE STATUS = 'AJS_ToTransferForUser'");
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
    auto stmt = conn.createStmt("SELECT TOTAL_REPORT_RETRIES, IS_REPORTING, TOTAL_RETRIES"
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
  // Job must appear in ARCHIVE_FAILED_QUEUE exactly once, at ReadyForDeletion, with
  // TOTAL_REPORT_RETRIES=1. Together with the 0-rows check above this pins the move's core
  // invariant: each row is written to the failed queue once AND removed from the active queue.
  // Chaining the UPDATE, the INSERT and the DELETE into one statement broke exactly that - under a
  // single snapshot and command id PostgreSQL discards a DELETE of a row the same statement has
  // already updated, so the row was duplicated into the failed queue and stranded in the active
  // queue at ReadyForDeletion, where nothing collects it.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT, MAX(TOTAL_REPORT_RETRIES) AS MAX_RR FROM "
                                "ARCHIVE_FAILED_QUEUE WHERE STATUS = 'ReadyForDeletion'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
    ASSERT_EQ(1u, rset.columnUint64("MAX_RR"));
  }
}

// reportType must NOT influence ArchiveMount::failReportBatch()'s user/repack bucketing - only
// isRepack and the retry counters may. reportType is derived purely from the row's STATUS at
// initialize() time (see ArchiveRdbJob::initialize()'s switch), and every job reaching this
// function was fetched by getNextJobBatch() while STATUS was still AJS_ToTransferForUser -
// mapping to NoReportRequired - with nothing re-evaluating it before this call. A bucketing rule
// that special-cased NoReportRequired therefore fired on every USER job unconditionally (not just
// repack, where it happens to be the correct outcome), sending a single transient bookkeeping-
// write failure straight to ARCHIVE_FAILED_QUEUE without ever spending its report-retry budget.
// This test forces reportType to NoReportRequired directly (there is no real archive code path
// that sets it to this value for a reason other than that staleness) and asserts the job is
// requeued anyway, same as archiveFailReportBatchRequeue above.
TEST_P(RelationalDBTest, archiveFailReportBatch_reportTypeIgnoredForUserBucketing) {
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

  // Already NoReportRequired as fetched (see comment above); set explicitly so the test does not
  // depend on that fetch-time detail to make its point.
  jobs.front()->reportType = SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired;
  jobs.front()->prepareFailReport("report failure under budget despite NoReportRequired");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failReportBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  // Requeued, not failed out: totalReportRetries=1 is under the default maxReportRetries=2.
  {
    auto stmt = conn.createStmt("SELECT TOTAL_REPORT_RETRIES, IS_REPORTING FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("TOTAL_REPORT_RETRIES"));
    ASSERT_FALSE(rset.columnBool("IS_REPORTING"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
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
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1"
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
}

// When a single copy of a multi-copy archive request exceeds its report retry budget,
// failReportBatchExceedRetries must atomically move ALL copies of the same archive
// request to ARCHIVE_FAILED_QUEUE.
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
    auto stmt =
      conn.createStmt("SELECT REPORT_FAILURE_LOG, FAILURE_LOG FROM ARCHIVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
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
setupRetrieveJobForReportFailureTesting(cta::RelationalDBTestWrapper& db,
                                        cta::log::LogContext& lc,
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
  for (auto& job : jobs) {
    transferBatch.push_back(job.get());
  }
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
    auto stmt = conn.createStmt("SELECT TOTAL_REPORT_RETRIES, IS_REPORTING, TOTAL_RETRIES"
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
  // Job must appear in RETRIEVE_FAILED_QUEUE with TOTAL_REPORT_RETRIES=1, and with STATUS already
  // moved to ReadyForDeletion - failReportBatchExceedRetries() writes that status before moving
  // the row, and STATUS is the column `cta-admin failedrequest ls` reports.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT, MAX(TOTAL_REPORT_RETRIES) AS MAX_RR FROM "
                                "RETRIEVE_FAILED_QUEUE WHERE STATUS = 'ReadyForDeletion'");
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
    auto stmt = conn.createStmt("UPDATE RETRIEVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 1"
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
  for (auto& job : jobs) {
    transferBatch.push_back(job.get());
  }
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
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
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
    auto stmt =
      conn.createStmt("SELECT REPORT_FAILURE_LOG, FAILURE_LOG FROM RETRIEVE_ACTIVE_QUEUE WHERE IS_REPORTING = FALSE");
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

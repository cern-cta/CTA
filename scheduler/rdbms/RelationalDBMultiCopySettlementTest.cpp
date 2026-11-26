/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/TimingList.hpp"
#include "common/utils/Timer.hpp"
#include "scheduler/rdbms/RelationalDBTestFixture.hpp"

#include <list>
#include <memory>
#include <utility>
#include <vector>

// Covers the user archive multi-copy settlement rules documented in
// docs/content/dev/internals/shared-state/scheduler-db/postgresql/failure-handling.md under
// "Why user requests fail as a group": a copy that exceeds its transfer retries while a sibling
// has not yet been dispatched out of ARCHIVE_PENDING_QUEUE cannot be swept straight to the
// terminal AJS_ToReportToUserForFailure (nothing to force-settle the undispatched sibling with,
// and sweeping early would let it be reported and deleted before that sibling ever ran). It parks
// at AJS_WaitReplicasBeforeReportingFailureToDisk instead, mirroring the pre-existing success-side
// AJS_WaitReplicasBeforeReportingSuccessToDisk wait, until the last copy of the group settles -
// whichever way - and resolves the whole group together.

namespace unitTests {

TEST_P(RelationalDBTest, archiveMultiCopyTransferFailureParksAtWaitFailureWhenSiblingStillPending) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  // One 2-copy request, both copies routed to the same tape pool.
  queueMultiCopyArchiveJob(db, lc, 994001, "tapePoolSettleA", "SETTLE-A", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-A";
  tape.tapePool = "tapePoolSettleA";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;

  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  // Fetch only one of the two copies: the other is left behind in ARCHIVE_PENDING_QUEUE,
  // undispatched, exactly as if the mount's task queue only had room for one more file.
  auto jobs = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobs.size());

  jobs.front()->prepareFailTransfer("simulated terminal transfer failure, sibling still pending");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.front().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // Parked, not terminal: the sibling isn't visible yet, so completeness can't be decided.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    // The undispatched sibling is untouched - still sitting in ARCHIVE_PENDING_QUEUE.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
                                " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // flagReportingJobsByStatus() is a plain status filter now (no completeness check of its own,
  // see getNextArchiveJobsToReportBatch()) - a parked row must not be picked up.
  auto reportBatch = db.getNextArchiveJobsToReportBatch(10, lc);
  ASSERT_EQ(0u, reportBatch.size());
}

TEST_P(RelationalDBTest, archiveMultiCopyGroupResolvesToFailureOncePendingSiblingAlsoFails) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994002, "tapePoolSettleB", "SETTLE-B", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-B";
  tape.tapePool = "tapePoolSettleB";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // First copy fails while the second is still pending - parks, as in the test above.
  auto jobsA = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  jobsA.front()->prepareFailTransfer("first copy fails, sibling still pending");
  std::vector<SchedulerDatabase::ArchiveJob*> batchA = {jobsA.front().get()};
  mount->failTransferBatch(batchA, lc);

  // The second copy is now dispatched and also exceeds its retries.
  auto jobsB = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());
  jobsB.front()->prepareFailTransfer("second copy also fails - group now fully visible");
  std::vector<SchedulerDatabase::ArchiveJob*> batchB = {jobsB.front().get()};
  mount->failTransferBatch(batchB, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // Every copy is now present, so the whole group - including the one already parked - settles
    // to the terminal failure status together, in the second call.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }

  // Now that the group is complete, both rows are reportable in one pass.
  auto reportBatch = db.getNextArchiveJobsToReportBatch(10, lc);
  ASSERT_EQ(2u, reportBatch.size());
}

// The copy that was still pending when its sibling failed goes on to transfer successfully. The
// group must still end up failed - a late success cannot resurrect a request that already lost a
// copy.
TEST_P(RelationalDBTest, archiveMultiCopyGroupResolvesToFailureWhenPendingSiblingLaterSucceeds) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994003, "tapePoolSettleC", "SETTLE-C", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-C";
  tape.tapePool = "tapePoolSettleC";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // Copy A fails while copy B is still undispatched - parks at the wait-failure status.
  auto jobsA = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  jobsA.front()->prepareFailTransfer("copy A fails, copy B not dispatched yet");
  std::vector<SchedulerDatabase::ArchiveJob*> batchA = {jobsA.front().get()};
  mount->failTransferBatch(batchA, lc);

  // Copy B is dispatched afterwards and its transfer succeeds - driven directly via the DB-level
  // mount's setJobBatchTransferred(), the same entry point ArchiveMount::reportJobsBatchTransferred()
  // calls after a real tape write and catalogue update both succeed (see
  // archiveMountHeartbeatRefreshedOnReportNotJustFetch for the same pattern).
  auto jobsB = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());
  mount->setJobBatchTransferred(jobsB, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // Both copies - the one that failed and the one that went on to succeed - end up failed
    // together. Neither AJS_WaitReplicasBeforeReportingSuccessToDisk nor
    // AJS_WaitReplicasBeforeReportingFailureToDisk should have any row left for this request.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForSuccess'"
                                " OR STATUS = 'AJS_WaitReplicasBeforeReportingSuccessToDisk'"
                                " OR STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// Symmetric check on the success side, and a regression check that a still-transferring sibling
// is still force-settled immediately when the other copy fails - the new wait status only changes
// what an undispatched sibling does, not this pre-existing behaviour (see "Why user requests fail
// as a group" in failure-handling.md).
TEST_P(RelationalDBTest, archiveMultiCopySuccessParksThenFailingSiblingSweepsBothToFailure) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994004, "tapePoolSettleD", "SETTLE-D", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-D";
  tape.tapePool = "tapePoolSettleD";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // Both copies dispatched together this time - neither is left behind in ARCHIVE_PENDING_QUEUE.
  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Copy A succeeds. Copy B is left alone, still AJS_ToTransferForUser, as if a drive were still
  // physically writing it.
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> jobsA;
  jobsA.push_back(std::move(jobs.front()));
  jobs.pop_front();
  mount->setJobBatchTransferred(jobsA, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // A parks: B hasn't settled (it's still mid-transfer), so completeness isn't met yet.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingSuccessToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // Copy B now exceeds its retries. failTransferBatchExceedRetries() matches by
  // ARCHIVE_REQUEST_ID with no STATUS filter, so it reaches copy A's parked row too, even though
  // copy A's own transfer already succeeded - interrupting a still-"writing" sibling is
  // immediate and unconditional.
  jobs.front()->prepareFailTransfer("copy B fails after copy A already succeeded and parked");
  std::vector<SchedulerDatabase::ArchiveJob*> batchB = {jobs.front().get()};
  mount->failTransferBatch(batchB, lc);

  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingSuccessToDisk'"
                                " OR STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// ArchiveJobQueueRow::requeueJobBatch() - used by TapeWriteSingleThread::
// requeueUnprocessedTasks() after a session-level abort, and by MigrationReportPacker::
// ReportLastBatchError::execute() - must not resurrect a row a sibling has already collaterally
// settled via failTransferBatchExceedRetries()'s ARCHIVE_REQUEST_ID-wide UPDATE (see "Why user
// requests fail as a group" in failure-handling.md). Its STATUS guard rejects a requeue for a row
// that is no longer at the active-transfer status the caller's stale in-memory job list assumed.
TEST_P(RelationalDBTest, archiveMultiCopyCrashRequeueCannotResurrectACollaterallyFailedSibling) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994005, "tapePoolSettleE", "SETTLE-E", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-E";
  tape.tapePool = "tapePoolSettleE";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // Both copies dispatched together - both visible in ARCHIVE_ACTIVE_QUEUE.
  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());
  const uint64_t jobIdA = jobs.front()->jobID;

  // Copy B exceeds its retries. Both copies are present, so the group settles immediately to the
  // terminal failure status together (no pending sibling here - this is the "both already
  // dispatched" case, same as archiveFailTransferBatchExceedRetries_multiCopy).
  jobs.back()->prepareFailTransfer("copy B fails, both copies already dispatched");
  std::vector<SchedulerDatabase::ArchiveJob*> batch = {jobs.back().get()};
  mount->failTransferBatch(batch, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }

  // Simulate a session-level abort on copy A's drive discovering, too late, that copy A was never
  // actually attempted - exactly what TapeWriteSingleThread::requeueUnprocessedTasks() does. Its
  // job-ID list was built before copy B's failure swept copy A's row too, so it still names
  // copy A's JOB_ID.
  std::list<std::string> staleJobIds = {std::to_string(jobIdA)};
  uint64_t nrows = mount->requeueJobBatch(staleJobIds, lc);

  // The guard must reject the stale requeue: 0 rows affected, copy A's row still exactly where
  // the group settlement left it.
  ASSERT_EQ(0u, nrows);
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

// Happy path, not exercised by any existing test: both copies of a user multi-copy request
// transfer successfully. Both must resolve straight to the terminal success status together and
// be reportable in one pass - no copy should be left parked at
// AJS_WaitReplicasBeforeReportingSuccessToDisk once its sibling has also settled.
TEST_P(RelationalDBTest, archiveMultiCopyBothCopiesSucceedTogether) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994006, "tapePoolSettleF", "SETTLE-F", 1000);
  db.waitSubthreadsComplete();

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-F";
  tape.tapePool = "tapePoolSettleF";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());

  // Both copies succeed in the same batch.
  mount->setJobBatchTransferred(jobs, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForSuccess'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingSuccessToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }

  auto reportBatch = db.getNextArchiveJobsToReportBatch(10, lc);
  ASSERT_EQ(2u, reportBatch.size());
}

// A 3-copy request where one copy (A) has already failed and parked (its third sibling, C, is
// still undispatched so the group cannot be complete yet), and a second copy (B) now succeeds.
// The group is still incomplete (only A and B are settled; C is not), so updateMultiCopyJobSuccess()
// must only park B - it must not touch A's row. Touching every row sharing the ARCHIVE_REQUEST_ID
// in that case would overwrite A's recorded failure with the success-wait status, erasing it.
TEST_P(RelationalDBTest, archiveMultiCopyIncompleteGroupSuccessDoesNotEraseAParkedSiblingFailure) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueTripleCopyArchiveJob(db, lc, 994007, "tapePoolSettleG", "SETTLE-G", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_TOTAL_RETRIES = 1");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-G";
  tape.tapePool = "tapePoolSettleG";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // Dispatch only 2 of the 3 copies - the third (C) stays undispatched in ARCHIVE_PENDING_QUEUE.
  auto jobsAB = mount->getNextJobBatch(2, 1'000'000, lc);
  ASSERT_EQ(2u, jobsAB.size());

  // Copy A fails. present_count (2, A and B) < REQUEST_JOB_COUNT (3), so A parks rather than
  // going straight to the terminal failure status.
  jobsAB.front()->prepareFailTransfer("copy A fails, copy C still undispatched");
  std::vector<SchedulerDatabase::ArchiveJob*> batchA = {jobsAB.front().get()};
  mount->failTransferBatch(batchA, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // Copy B now succeeds. The group is still incomplete (C has not even been dispatched), so B
  // must park alone.
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> jobsB;
  jobsB.push_back(std::move(jobsAB.back()));
  mount->setJobBatchTransferred(jobsB, lc);

  // A's parked failure must survive untouched - this is the regression check.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // B parks at the success-wait status, not promoted (C still hasn't settled).
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingSuccessToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  // Nothing has been prematurely promoted to a terminal status.
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_ToReportToUserForFailure'"
                                " OR STATUS = 'AJS_ToReportToUserForSuccess'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// ArchiveMount::failReportBatch()'s "exceed" branch for a USER job is reached when
// setJobBatchTransferred() itself failed - the row's completeness was never determined (unlike
// RelationalDB::failArchiveJobBatchReport()'s caller, which only ever sees already-complete
// groups fetched by getNextArchiveJobsToReportBatch() - see that function's own comment).
// failReportBatchExceedRetries()'s user branch must therefore run the same sibling-presence check
// failTransferBatchExceedRetries() already does, or it would sweep an incomplete group straight
// to ARCHIVE_FAILED_QUEUE while a sibling is still undispatched.
TEST_P(RelationalDBTest, archiveMultiCopyReportFailureParksAtWaitFailureWhenSiblingStillPending) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994008, "tapePoolSettleH", "SETTLE-H", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 0");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  ASSERT_EQ(1u, mountInfo->potentialMounts.size());

  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-H";
  tape.tapePool = "tapePoolSettleH";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");
  ASSERT_NE(nullptr, mount);

  // Fetch only one of the two copies: the other is left behind in ARCHIVE_PENDING_QUEUE,
  // undispatched - exactly as in the transfer-failure parking test above, but this time the
  // bookkeeping WRITE itself (setJobBatchTransferred()) is what failed, routing through
  // failReportBatch()'s "exceed" branch instead of failTransferBatch().
  auto jobsA = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsA.size());
  jobsA.front()->prepareFailReport("simulated exhausted bookkeeping write retries, sibling still pending");
  std::vector<SchedulerDatabase::ArchiveJob*> batchA = {jobsA.front().get()};
  mount->failReportBatch(batchA, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    // Parked, not swept: the sibling isn't visible yet, so completeness can't be decided.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_PENDING_QUEUE"
                                " WHERE STATUS = 'AJS_ToTransferForUser'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // The second copy is now dispatched and its bookkeeping write also exceeds its retries - the
  // group is now fully present, so this call sweeps both rows (the one just failing and the one
  // already parked) into ARCHIVE_FAILED_QUEUE together.
  auto jobsB = mount->getNextJobBatch(1, 1'000'000, lc);
  ASSERT_EQ(1u, jobsB.size());
  jobsB.front()->prepareFailReport("second copy also exhausts report retries - group now fully visible");
  std::vector<SchedulerDatabase::ArchiveJob*> batchB = {jobsB.front().get()};
  mount->failReportBatch(batchB, lc);

  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(2u, rset.columnUint64("CNT"));
  }
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE"
                                " WHERE STATUS = 'AJS_WaitReplicasBeforeReportingFailureToDisk'");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(0u, rset.columnUint64("CNT"));
  }
}

// failReportBatchExceedRetries()'s pending-sibling park (added for the report-failure parking fix
// above) must NOT apply once the group's outcome was already decided before this call.
// RelationalDB::failArchiveJobBatchReport() - unlike ArchiveMount::failReportBatch() - is fed only
// by getNextArchiveJobsToReportBatch(), which only ever returns rows whose STATUS is already
// AJS_ToReportToUserForSuccess/ForFailure: by the time a row gets there, its whole group was
// already settled atomically. A missing sibling row at that point means it was already reported
// and deleted, not that it is still pending - parking here would wait forever for a sibling that
// will never return.
TEST_P(RelationalDBTest, archiveMultiCopyReportExceedRetriesDoesNotParkOnceGroupAlreadyDecided) {
  using namespace cta;

  auto logger = makeLogger();
  log::LogContext lc(*logger);
  auto& db = getDb();

  queueMultiCopyArchiveJob(db, lc, 994009, "tapePoolSettleI", "SETTLE-I", 1000);
  db.waitSubthreadsComplete();

  {
    auto conn = db.getRelationalDB().getConn();
    auto stmt = conn.createStmt("UPDATE ARCHIVE_PENDING_QUEUE SET MAX_REPORT_RETRIES = 0");
    stmt.executeNonQuery();
  }

  auto mountInfo = db.getMountInfo(lc);
  catalogue::TapeForWriting tape;
  tape.vid = "V-SETTLE-I";
  tape.tapePool = "tapePoolSettleI";
  tape.mediaType = "LTO9";
  tape.vendor = "vendor";
  tape.vo = "vo";
  tape.capacityInBytes = 10ULL * 1024 * 1024 * 1024;
  auto mount = mountInfo->createArchiveMount(mountInfo->potentialMounts.front(), tape, "drive", "loglib", "host");

  // Both copies succeed together - the group settles atomically to AJS_ToReportToUserForSuccess.
  auto jobs = mount->getNextJobBatch(10, 1'000'000, lc);
  ASSERT_EQ(2u, jobs.size());
  mount->setJobBatchTransferred(jobs, lc);

  // Both rows are now independently reportable.
  auto reportBatch = db.getNextArchiveJobsToReportBatch(10, lc);
  ASSERT_EQ(2u, reportBatch.size());

  // Copy A's EOS report succeeds - its row is deleted.
  std::list<SchedulerDatabase::ArchiveJob*> listA {reportBatch.front().get()};
  log::TimingList timingsA;
  cta::utils::Timer timerA;
  db.setArchiveJobBatchReported(listA, timingsA, timerA, lc);

  auto conn = db.getRelationalDB().getConn();
  {
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_ACTIVE_QUEUE");
    auto rset = stmt.executeQuery();
    ASSERT_TRUE(rset.next());
    ASSERT_EQ(1u, rset.columnUint64("CNT"));
  }

  // Copy B's EOS report keeps failing until its report-retry budget (0) is immediately exceeded -
  // the sibling (copy A) is already gone by now.
  std::vector<SchedulerDatabase::ArchiveJob*> batchB {reportBatch.back().get()};
  db.failArchiveJobBatchReport(batchB, lc);

  {
    // Copy B must have moved straight to ARCHIVE_FAILED_QUEUE, not parked waiting for copy A.
    auto stmt = conn.createStmt("SELECT COUNT(*) AS CNT FROM ARCHIVE_FAILED_QUEUE");
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

// updateMultiCopyJobSuccess()'s replay-demotion guard (the user-archive equivalent of the repack
// replay-demotion fix - see repackUpdateSuccessReplayDoesNotDemoteAlreadyReadyForDeletionSibling
// in RelationalDBTest.cpp) has NO regression test here: reproducing a replay needs a second,
// independently-constructed ArchiveJob object addressing the same already-settled JOB_ID, and
// ArchiveRdbJob has no public constructor that takes just a job ID - only (ConnPool&, Rset, bool)
// from a real fetched row, or (ConnPool&) empty, and RelationalDB exposes no public ConnPool&
// accessor to build one from a test. getNextJobBatch() won't refetch the row either, since its
// STATUS has already moved on. Verified instead by hand-tracing updateMultiCopyJobSuccess()'s SQL
// directly: a replayed target already at AJS_ToReportToUserForSuccess failed the same way
// updateRepackJobSuccess()'s pre-fix ReadyForDeletion replay did, by the identical mechanism (no
// STATUS guard on the target selection), and is fixed the same way (the new :STATUS_ALREADY_SUCCESS
// exclusion on the final UPDATE's WHERE, symmetric with the pre-existing :STATUS_FAILED4 one).

}  // namespace unitTests

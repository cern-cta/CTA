/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"

#include "common/utils/utils.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"
#include "scheduler/rdbms/ArchiveMount.hpp"

#include <set>
#include <utility>

namespace cta::schedulerdb::postgres {

namespace {
// Row-locks (FOR UPDATE) every row of each given (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) repack
// group, regardless of that row's own status, in ONE round trip. This is what serializes two
// concurrent updateRepackJobSuccess() calls completing different copies of the same file - see
// that function's own comment for the full reasoning.
//
// Deliberately NOT a Postgres advisory lock: advisory locks always go through the heavyweight
// shared-memory lock manager (bounded by max_locks_per_transaction, default 64), costing a slot
// for as long as they're held regardless of contention. A repack batch can touch hundreds of
// distinct files in one transaction, which would blow past that default routinely - turning a
// rare race into a routine "out of shared memory" failure. Native row-level locks don't have
// this problem: Postgres records them directly on the tuple (via xmax), not in the shared lock
// table, unless there's actual contention - exactly the rare case this exists to handle, not the
// common one. This is the mechanism Postgres's MVCC design is built to scale for.
//
// ORDER BY forces a deterministic row-locking order (also valid combined with FOR UPDATE, and
// the standard documented Postgres idiom for avoiding deadlocks when locking multiple rows) so
// two concurrent calls touching overlapping groups always lock in the same relative order.
//
// The result set is fully drained (not just executed) because that's what actually causes every
// matching row to be visited and locked - the SELECT list itself is unused.
void lockRepackGroupRows(Transaction& txn, const std::set<std::pair<uint64_t, uint64_t>>& groupKeys) {
  if (groupKeys.empty()) {
    return;
  }
  std::string pairs;
  for (size_t i = 0; i < groupKeys.size(); ++i) {
    if (i > 0) {
      pairs += ",";
    }
    const std::string s = std::to_string(i);
    pairs += "(:R" + s + "::BIGINT, :F" + s + "::BIGINT)";
  }
  std::string sql = "SELECT JOB_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                    "WHERE (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) IN ("
                    + pairs
                    + ") "
                      "ORDER BY REPACK_REQUEST_ID, ARCHIVE_FILE_ID, JOB_ID FOR UPDATE";
  auto stmt = txn.getConn().createStmt(sql);
  size_t i = 0;
  for (const auto& [repackRequestId, archiveFileId] : groupKeys) {
    const std::string s = std::to_string(i);
    stmt.bindUint64(":R" + s, repackRequestId);
    stmt.bindUint64(":F" + s, archiveFileId);
    ++i;
  }
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbLock);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    // Draining is the point - see function comment. Nothing to do with each row.
  }
}

// Row-locks (FOR UPDATE) every row of each given ARCHIVE_REQUEST_ID user multi-copy group,
// regardless of that row's own status, in ONE round trip, in sorted order - the archive-side
// mirror of lockRepackGroupRows() above, for the same reason: two concurrent
// updateMultiCopyJobSuccess() calls completing different copies of the same request must not
// both read "sibling not settled yet" and both leave the group parked. Locking first forces the
// second call to wait for the first to commit, then read its fresh, post-commit state - see that
// function's own comment for the full reasoning, which applies here unchanged.
void lockArchiveRequestGroupRows(Transaction& txn, const std::set<uint64_t>& requestIds) {
  if (requestIds.empty()) {
    return;
  }
  std::string ids;
  for (size_t i = 0; i < requestIds.size(); ++i) {
    if (i > 0) {
      ids += ",";
    }
    ids += ":R" + std::to_string(i);
  }
  std::string sql = "SELECT JOB_ID FROM ARCHIVE_ACTIVE_QUEUE "
                    "WHERE ARCHIVE_REQUEST_ID IN ("
                    + ids
                    + ") "
                      "ORDER BY ARCHIVE_REQUEST_ID, JOB_ID FOR UPDATE";
  auto stmt = txn.getConn().createStmt(sql);
  size_t i = 0;
  for (const auto& requestId : requestIds) {
    stmt.bindUint64(":R" + std::to_string(i), requestId);
    ++i;
  }
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbLock);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    // Draining is the point - see lockRepackGroupRows()'s comment. Nothing to do with each row.
  }
}
}  // namespace

rdbms::Rset ArchiveJobQueueRow::moveJobsToDbActiveQueue(Transaction& txn,
                                                        ArchiveJobStatus newStatus,
                                                        const SchedulerDatabase::ArchiveMount::MountInfo& mountInfo,
                                                        uint64_t maxBytesRequested,
                                                        uint64_t limit,
                                                        bool isRepack) {
  /* using write row lock FOR UPDATE for the select statement
   * since it is the same lock used for UPDATE
   * we first apply the LIMIT on the selection to limit
   * the number of rows and only after calculate the
   * running cumulative sum of bytes in the consequent step */
  std::string prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH SET_SELECTION AS (
      SELECT JOB_ID, ARCHIVE_FILE_ID, PRIORITY, SIZE_IN_BYTES
      FROM
    )SQL";
  sql += prefix + "ARCHIVE_PENDING_QUEUE ";
  sql += R"SQL(
      WHERE TAPE_POOL = :TAPE_POOL
      AND STATUS = :STATUS
      AND ( MOUNT_ID IS NULL OR MOUNT_ID = :SAME_MOUNT_ID )
      ORDER BY PRIORITY DESC, ARCHIVE_FILE_ID
      LIMIT :LIMIT
      FOR UPDATE SKIP LOCKED
    ),
    SELECTION_WITH_CUMULATIVE_SUMS AS (
      SELECT JOB_ID FROM (
        SELECT JOB_ID, SUM(SIZE_IN_BYTES) OVER (ORDER BY PRIORITY DESC, JOB_ID) AS CUMULATIVE_SIZE
          FROM SET_SELECTION ) s
      WHERE s.CUMULATIVE_SIZE <= :BYTES_REQUESTED
    ),
    MOVED_ROWS AS (
        DELETE FROM
      )SQL";
  sql += prefix + "ARCHIVE_PENDING_QUEUE ";
  sql += R"SQL( AIQ
        USING SELECTION_WITH_CUMULATIVE_SUMS CSEL
        WHERE AIQ.JOB_ID = CSEL.JOB_ID
        RETURNING AIQ.*
    )
    INSERT INTO
    )SQL";
  sql += prefix + "ARCHIVE_ACTIVE_QUEUE (";
  if (isRepack) {
    sql += " REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
        JOB_ID,
        ARCHIVE_REQUEST_ID,
        REQUEST_JOB_COUNT,
        STATUS,
        TAPE_POOL,
        MOUNT_POLICY,
        PRIORITY,
        MIN_ARCHIVE_REQUEST_AGE,
        ARCHIVE_FILE_ID,
        SIZE_IN_BYTES,
        COPY_NB,
        START_TIME,
        CHECKSUMBLOB,
        CREATION_TIME,
        DISK_INSTANCE,
        DISK_FILE_ID,
        DISK_FILE_OWNER_UID,
        DISK_FILE_GID,
        DISK_FILE_PATH,
        ARCHIVE_REPORT_URL,
        ARCHIVE_ERROR_REPORT_URL,
        REQUESTER_NAME,
        REQUESTER_GROUP,
        SRC_URL,
        STORAGE_CLASS,
        RETRIES_WITHIN_MOUNT,
        MAX_RETRIES_WITHIN_MOUNT,
        TOTAL_RETRIES,
        LAST_MOUNT_WITH_FAILURE,
        MAX_TOTAL_RETRIES,
        TOTAL_REPORT_RETRIES,
        MAX_REPORT_RETRIES,
        MOUNT_ID,
        VID,
        DRIVE,
        HOST,
        MOUNT_TYPE,
        LOGICAL_LIBRARY)
            SELECT
  )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
  }
  sql += R"SQL( M.JOB_ID,
                M.ARCHIVE_REQUEST_ID,
                M.REQUEST_JOB_COUNT,
                M.STATUS,
                M.TAPE_POOL,
                M.MOUNT_POLICY,
                M.PRIORITY,
                M.MIN_ARCHIVE_REQUEST_AGE,
                M.ARCHIVE_FILE_ID,
                M.SIZE_IN_BYTES,
                M.COPY_NB,
                M.START_TIME,
                M.CHECKSUMBLOB,
                M.CREATION_TIME,
                M.DISK_INSTANCE,
                M.DISK_FILE_ID,
                M.DISK_FILE_OWNER_UID,
                M.DISK_FILE_GID,
                M.DISK_FILE_PATH,
                M.ARCHIVE_REPORT_URL,
                M.ARCHIVE_ERROR_REPORT_URL,
                M.REQUESTER_NAME,
                M.REQUESTER_GROUP,
                M.SRC_URL,
                M.STORAGE_CLASS,
                M.RETRIES_WITHIN_MOUNT,
                M.MAX_RETRIES_WITHIN_MOUNT,
                M.TOTAL_RETRIES,
                M.LAST_MOUNT_WITH_FAILURE,
                M.MAX_TOTAL_RETRIES,
                M.TOTAL_REPORT_RETRIES,
                M.MAX_REPORT_RETRIES,
                :MOUNT_ID AS MOUNT_ID,
                :VID AS VID,
                :DRIVE AS DRIVE,
                :HOST AS HOST,
                :MOUNT_TYPE AS MOUNT_TYPE,
                :LOGICAL_LIBRARY AS LOGICAL_LIBRARY
            FROM MOVED_ROWS M
    RETURNING *;
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":TAPE_POOL", mountInfo.tapePool);
  stmt.bindString(":STATUS", to_string(newStatus));
  stmt.bindUint32(":LIMIT", limit);
  stmt.bindUint64(":MOUNT_ID", mountInfo.mountId);
  stmt.bindUint64(":SAME_MOUNT_ID", mountInfo.mountId);
  stmt.bindString(":VID", mountInfo.vid);
  stmt.bindString(":DRIVE", mountInfo.drive);
  stmt.bindString(":HOST", mountInfo.host);
  stmt.bindString(":MOUNT_TYPE", cta::common::dataStructures::toString(mountInfo.mountType));
  stmt.bindString(":LOGICAL_LIBRARY", mountInfo.logicalLibrary);
  stmt.bindUint64(":BYTES_REQUESTED", maxBytesRequested);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveToActive);
  auto result = stmt.executeQuery();
  return result;
}

uint64_t ArchiveJobQueueRow::updateMultiCopyJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  ArchiveJobStatus newStatus = ArchiveJobStatus::AJS_ToReportToUserForSuccess;
  std::string jobids_sqlpart;
  for (const auto& piece : jobIDs) {
    if (!jobids_sqlpart.empty()) {
      jobids_sqlpart += ",";
    }
    jobids_sqlpart += piece;
  }

  // Find which ARCHIVE_REQUEST_IDs this call touches, then lock every row of each - in sorted
  // order, before reading any completion state - exactly as updateRepackJobSuccess() does for
  // repack (see lockArchiveRequestGroupRows()'s comment and that function's own, for the full
  // reasoning). Without this, two mounts finishing the last two copies of a 2-copy request
  // concurrently can each read the other as "not settled yet" before either commits, and both
  // park their own copy instead of either promoting the group - stranding it indefinitely.
  std::set<uint64_t> requestIds;
  {
    std::string sql =
      "SELECT DISTINCT ARCHIVE_REQUEST_ID FROM ARCHIVE_ACTIVE_QUEUE WHERE JOB_ID IN (" + jobids_sqlpart + ")";
    auto stmt = txn.getConn().createStmt(sql);
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbSelectArchiveMulticopy);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      requestIds.insert(rset.columnUint64NoOpt("ARCHIVE_REQUEST_ID"));
    }
  }
  if (requestIds.empty()) {
    return 0;
  }
  lockArchiveRequestGroupRows(txn, requestIds);

  /* A target row just transferred successfully. Whether it (and the rest of its multi-copy
   * request) is now ready to report to disk depends on every copy of the request having
   * individually settled - success (parked at AJS_WaitReplicasBeforeReportingSuccessToDisk,
   * waiting for the others) or failure (parked at AJS_WaitReplicasBeforeReportingFailureToDisk, or
   * already fully terminal at AJS_ToReportToUserForFailure if failTransferBatchExceedRetries()
   * found every copy already present - see that function, which is this one's mirror image on the
   * failure side). sibling_settlement counts all three as "settled" (any_failed records whether any
   * of them is a failure outcome): a sibling parked at either wait status, or already terminally
   * failed, all count toward REQUEST_JOB_COUNT here, so a copy that permanently failed earlier is
   * never lost from the count just because its row is no longer mid-transfer.
   *
   * This mirrors moveFailedRepackJobBatchToFailedQueueTable()'s settled_siblings/
   * complete_failed_groups pattern (STATUS IN success, failure), which already gets this right for
   * repack.
   *
   * The final UPDATE's WHERE is deliberately asymmetric between the complete and incomplete
   * cases. When the group is NOT yet complete, only the literal target row(s) (aj2.JOB_ID =
   * tsm.JOB_ID) are touched, parking them at the wait-for-success status - a sibling already
   * parked at either wait status (or already terminally failed) must be left untouched here, or
   * this UPDATE would silently overwrite a sibling's recorded failure with "waiting for success"
   * and erase it. Only once the group IS complete (rg.ARCHIVE_REQUEST_ID IS NOT NULL) does the
   * WHERE widen to every row sharing the ARCHIVE_REQUEST_ID, because that is the one moment every
   * sibling's row - including one already parked from an earlier call - needs to move to the
   * group's final verdict together, in this one statement. Request-wide outcomes settle exactly
   * once, in whichever call turns out to be the group's last. Because of that,
   * flagReportingJobsByStatus() never needs to re-derive completeness itself: by the time any row
   * reaches a terminal status, the whole group was already set to a terminal status together in
   * this one statement - reporting is free to just pick up rows whose STATUS says so, with no race
   * against settlement. aj2.STATUS <> FAILED additionally excludes a row that already
   * independently reached AJS_ToReportToUserForFailure, and aj2.STATUS <> this function's own
   * newStatus excludes one that already reached AJS_ToReportToUserForSuccess (a replayed success -
   * see that bind's own comment below for why this one matters) - both are already correctly
   * decided and do not need re-touching, only counting (which they get via the UNION ALL above,
   * not via this final UPDATE's WHERE).
   */
  std::string sql = R"SQL(
      WITH target_success_multicopy AS (
        SELECT JOB_ID, ARCHIVE_REQUEST_ID, STATUS, REQUEST_JOB_COUNT
        FROM ARCHIVE_ACTIVE_QUEUE scj
        WHERE scj.JOB_ID IN (
      )SQL";
  sql += jobids_sqlpart;
  sql += R"SQL( )
      ),
      sibling_settlement AS (
        -- COUNT/BOOL_OR(DISTINCT ...): the batch passed into this call can itself carry two
        -- siblings of the same multi-copy request (two copies succeeding in the same poll), which
        -- would otherwise join a third sibling once per such target row below and double-count it.
        SELECT combined.ARCHIVE_REQUEST_ID,
               COUNT(DISTINCT combined.JOB_ID) AS settled_count,
               MAX(combined.REQUEST_JOB_COUNT) AS expected_count,
               BOOL_OR(combined.STATUS = ANY(ARRAY[:STATUS_COND_REPLICAS_FAILURE, :STATUS_FAILED]
                                             ::ARCHIVE_JOB_STATUS[])) AS any_failed
        FROM (
          SELECT t.JOB_ID, t.ARCHIVE_REQUEST_ID, t.STATUS, t.REQUEST_JOB_COUNT
          FROM target_success_multicopy t

          UNION ALL

          SELECT aj.JOB_ID, aj.ARCHIVE_REQUEST_ID, aj.STATUS, aj.REQUEST_JOB_COUNT
          FROM ARCHIVE_ACTIVE_QUEUE aj
          JOIN target_success_multicopy t USING (ARCHIVE_REQUEST_ID)
          WHERE aj.JOB_ID <> t.JOB_ID
            AND aj.STATUS = ANY(ARRAY[:STATUS_COND_REPLICAS, :STATUS_COND_REPLICAS_FAILURE2, :STATUS_FAILED2]
                                ::ARCHIVE_JOB_STATUS[])
        ) AS combined
        GROUP BY combined.ARCHIVE_REQUEST_ID
      ),
      ready_groups AS (
        SELECT ARCHIVE_REQUEST_ID, any_failed
        FROM sibling_settlement
        WHERE settled_count >= expected_count
      )
      UPDATE ARCHIVE_ACTIVE_QUEUE aj2
      SET STATUS = CASE
          WHEN rg.ARCHIVE_REQUEST_ID IS NOT NULL AND rg.any_failed
            THEN :STATUS_FAILED3::ARCHIVE_JOB_STATUS
          WHEN rg.ARCHIVE_REQUEST_ID IS NOT NULL
            THEN :STATUS_READY_FOR_REPORTING::ARCHIVE_JOB_STATUS
          ELSE :STATUS_WAIT_FOR_ALL_BEFORE_REPORT::ARCHIVE_JOB_STATUS
      END,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      FROM target_success_multicopy tsm
          LEFT JOIN ready_groups rg
              ON rg.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
      WHERE aj2.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
        AND (aj2.JOB_ID = tsm.JOB_ID OR rg.ARCHIVE_REQUEST_ID IS NOT NULL)
        AND aj2.STATUS <> :STATUS_FAILED4::ARCHIVE_JOB_STATUS
        AND aj2.STATUS <> :STATUS_ALREADY_SUCCESS::ARCHIVE_JOB_STATUS;
  )SQL";
  // AJS_ToReportToUserForSuccess (this function's own destination) is monotonic exactly like
  // ReadyForDeletion is for updateRepackJobSuccess(): once a row reaches it, this function must
  // never target that row again. A caller can replay the same success more than once -
  // ArchiveMount::reportJobsBatchTransferred() wraps setJobBatchTransferred() in
  // retryWithBackoff(), so a commit that actually succeeded but whose acknowledgement was lost to
  // the caller gets retried with the same job IDs. Without this exclusion, a replay would find its
  // target via the plain JOB_ID match (aj2.STATUS <> FAILED4 alone does not stop it), see its own
  // group as "incomplete" (sibling_settlement's sibling branch only counts a sibling still at a
  // wait status or already terminally failed - not one already at this function's own success
  // destination, since by definition a sibling that got there was never "waiting"), and demote the
  // row back to AJS_WaitReplicasBeforeReportingSuccessToDisk while its sibling - possibly already
  // reported and deleted by then - never returns to complete the group again.
  //
  // Four distinct placeholder names bind the same AJS_ToReportToUserForFailure value
  // (:STATUS_FAILED/2/3/4), two bind AJS_WaitReplicasBeforeReportingFailureToDisk
  // (:STATUS_COND_REPLICAS_FAILURE/2), and :STATUS_ALREADY_SUCCESS binds the same newStatus value
  // as :STATUS_READY_FOR_REPORTING - the statement parser (ParamNameToIdx) rejects a repeated
  // :name as a duplicate, so every textual occurrence of the same value needs its own name here,
  // not just a stylistic preference.
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS_READY_FOR_REPORTING", to_string(newStatus));
  stmt.bindString(":STATUS_ALREADY_SUCCESS", to_string(newStatus));
  stmt.bindString(":STATUS_COND_REPLICAS", to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_COND_REPLICAS_FAILURE",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingFailureToDisk));
  stmt.bindString(":STATUS_COND_REPLICAS_FAILURE2",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingFailureToDisk));
  stmt.bindString(":STATUS_WAIT_FOR_ALL_BEFORE_REPORT",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_FAILED", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  stmt.bindString(":STATUS_FAILED2", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  stmt.bindString(":STATUS_FAILED3", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  stmt.bindString(":STATUS_FAILED4", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchiveMulticopy);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::updateJobStatus(Transaction& txn,
                                             ArchiveJobStatus newStatus,
                                             const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    sqlpart += piece + ",";
  }
  if (!sqlpart.empty()) {
    sqlpart.pop_back();
  }
  if (newStatus == ArchiveJobStatus::AJS_Complete || newStatus == ArchiveJobStatus::AJS_Failed
      || newStatus == ArchiveJobStatus::ReadyForDeletion) {
    std::string sql = R"SQL(
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
        WHERE
          JOB_ID IN (
        )SQL";
    sql += sqlpart + std::string(")");
    auto stmt1 = txn.getConn().createStmt(sql);
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
    stmt1.executeNonQuery();
    auto nrows = stmt1.getNbAffectedRows();
    txn.setRowCountForTelemetry(nrows);
    return nrows;
  }
  std::string sql = "UPDATE ARCHIVE_ACTIVE_QUEUE SET STATUS = :NEWSTATUS1::ARCHIVE_JOB_STATUS, ";
  sql += "        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT WHERE JOB_ID IN (" + sqlpart + ")";
  auto stmt2 = txn.getConn().createStmt(sql);
  stmt2.bindString(":NEWSTATUS1", to_string(newStatus));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  stmt2.executeNonQuery();
  auto nrows = stmt2.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

uint64_t ArchiveJobQueueRow::updateRepackJobSuccess(Transaction& txn, const std::vector<std::string>& jobIDs) {
  if (jobIDs.empty()) {
    return 0;
  }
  std::string sqlpart;
  for (const auto& piece : jobIDs) {
    if (!sqlpart.empty()) {
      sqlpart += ",";
    }
    sqlpart += piece;
  }
  /* For Repack, when we report success this query handles the check of all other replica
   * rows/jobs with the same (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) which needed to be archived too.
   * If all the rows in that group are in AJS_ToReportToRepackForSuccess except the one(s) being
   * currently updated, then update all of them to ReadyForDeletion. This signals the next step
   * that the source file can be deleted from disk; otherwise it updates the status just to
   * AJS_ToReportToRepackForSuccess.
   *
   * Single-copy jobs (REQUEST_JOB_COUNT = 1) need no sibling coordination and are settled
   * immediately below, in their own statement, before any locking.
   *
   * For multi-copy jobs, completion is decided by: row-lock (FOR UPDATE) every row of every
   * group this call touches, regardless of that row's own status, in a single round trip in
   * sorted order (see lockRepackGroupRows() - deliberately native row locks, not Postgres
   * advisory locks, which would cost a max_locks_per_transaction-counted slot per distinct file
   * regardless of contention; a repack batch can span hundreds of distinct files in one
   * transaction), THEN read completion state in a separate statement, THEN update. Two separate
   * statements rather than one combined FOR UPDATE SELECT, because whether a FOR UPDATE buried
   * inside one CTE of a larger multi-CTE statement is guaranteed to make the *rest* of that same
   * statement see the post-wait, freshly committed row is a subtle enough Postgres semantics
   * question that this isn't where to bet correctness on it; a lock taken in its own statement,
   * followed by a plain read in a new statement, gets an unambiguous fresh snapshot under READ
   * COMMITTED once the lock is granted - no FOR UPDATE needed in the read at all, and DISTINCT
   * becomes usable without the "FOR UPDATE forbids DISTINCT" restriction.
   *
   * Locking every group a batch touches, in sorted key order, rather than locking only this
   * call's own target rows, is what prevents two concurrent calls to this function - each
   * completing a DIFFERENT copy of the SAME file - from both reading "sibling not done yet" and
   * both committing without promoting the group: whichever call's lock acquisition goes first
   * holds it until its commit; the second blocks until then and only reads completion state
   * afterwards, seeing the first call's committed update. Sorting avoids a deadlock between two
   * batches that each span more than one group in different orders.
   *
   * report recovery (ArchiveMount::failReportBatch()'s repack "exceed" branch, which calls this
   * same function to recover a lost updateRepackJobSuccess() write - see that function's own
   * comment) automatically gets the same locking discipline, since it lives inside this function
   * rather than at each call site.
   *
   * The completeness count below uses COUNT(DISTINCT ...JOB_ID), not COUNT(*): the sibling join
   * is keyed on (REPACK_REQUEST_ID, ARCHIVE_FILE_ID), not JOB_ID, so one already-successful
   * sibling joins against EVERY target row sharing its file - if two copies of the same file both
   * succeed in this one call, that sibling would otherwise be counted twice, which can inflate
   * the total past REQUEST_JOB_COUNT while a genuinely still-transferring copy exists, promoting
   * an incomplete group to ReadyForDeletion. Targets and siblings are disjoint JOB_ID sets by
   * construction (aj.JOB_ID <> t.JOB_ID), so deduplicating can only remove genuine duplicates,
   * never under-count.
   *
   * The final UPDATE is restricted to aj2.JOB_ID IN the targets themselves (touched because
   * settling them is this call's own job - target_success_multicopy already excludes a target
   * at ReadyForDeletion, so "the targets" here never includes one) OR aj2.STATUS already =
   * AJS_ToReportToRepackForSuccess (an already-settled sibling, safe to promote or re-affirm).
   * It must NOT match a sibling purely by (REPACK_REQUEST_ID, ARCHIVE_FILE_ID)
   * regardless of that sibling's own status: a sibling still mid-transfer (STATUS still
   * AJS_ToTransferForRepack, actively owned by a live mount session on a different drive) would
   * otherwise also get matched and have its STATUS overwritten out from under that session.
   *
   * A transfer failure (AJS_ToReportToRepackForFailure, set directly by
   * failTransferBatchExceedRetries(), not through this function) is deliberately invisible to the
   * counting here: it is not a success-side "sibling for the moment", so a still-waiting
   * successful copy correctly neither stalls nor gets promoted on its own - it is
   * getNextFailedArchiveRepackReportBatch()'s own completeness check (see that function) that
   * eventually sweeps up the whole group, successes included, once every copy has settled and at
   * least one failed.
   *
   * The sibling lookup and the completeness count are scoped to (REPACK_REQUEST_ID,
   * ARCHIVE_FILE_ID), not ARCHIVE_FILE_ID alone, matching
   * moveFailedRepackJobBatchToFailedQueueTable(). The same file can be in two repack requests at
   * once - two tapes each holding one of its copies, both being repacked - and those rows are not
   * siblings: counting them together both inflates the count (so a group that really is complete
   * never promotes) and lets this UPDATE's STATUS = AJS_ToReportToRepackForSuccess term reach into
   * the other request's rows.
   *
   * HAVING uses >= rather than =, for the same reason it does there: with the group correctly
   * scoped the count cannot exceed REQUEST_JOB_COUNT, so the two forms agree - and if some future
   * path does produce a duplicate row, >= settles the group instead of stalling it forever.
   *
   * ReadyForDeletion is monotonic: once a row reaches it, this function must never target that
   * row again. A caller can replay the same success more than once - a mount session retrying
   * after it committed but never saw the acknowledgement, or report recovery
   * (failReportBatchExceedRetries()'s repack branch) replaying a write whose first attempt
   * actually went through - and without this, replaying a job whose group had already reached
   * ReadyForDeletion would find it via the plain JOB_ID match below (no STATUS check), see its
   * own group as "incomplete" (locked_siblings only counts AJS_ToReportToRepackForSuccess, not
   * ReadyForDeletion), and demote it back to AJS_ToReportToRepackForSuccess while its sibling
   * stays at ReadyForDeletion untouched. A maintd sweep that then collects the sibling deletes
   * the shared buffer file while the demoted row - which still needs that file as far as it
   * knows - is left behind with no sibling left to ever complete it again.
   */
  uint64_t nrows = 0;

  // Single-copy jobs: no sibling coordination needed, settle immediately. Already idempotent
  // against a replay: re-applying the same STATUS to a row already at it, or matching zero rows
  // once a row has moved on to ReadyForDeletion or beyond, is a no-op either way.
  {
    std::string sql = "UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE SET STATUS = :STATUS::ARCHIVE_JOB_STATUS "
                      "WHERE JOB_ID IN ("
                      + sqlpart + ") AND REQUEST_JOB_COUNT = 1";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":STATUS", to_string(ArchiveJobStatus::ReadyForDeletion));
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateRepackArchiveSuccess);
    stmt.executeNonQuery();
    nrows += stmt.getNbAffectedRows();
  }

  // Multi-copy jobs: find which (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) groups this call touches.
  // Excludes a target already at ReadyForDeletion - a replay of an already-completed group has
  // nothing left to do, and must not even take this group's lock: the group may legitimately be
  // owned right now by the maintd sweep that is about to delete it.
  std::set<std::pair<uint64_t, uint64_t>> groupKeys;
  {
    std::string sql = "SELECT REPACK_REQUEST_ID, ARCHIVE_FILE_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                      "WHERE JOB_ID IN ("
                      + sqlpart + ") AND REQUEST_JOB_COUNT > 1 AND STATUS <> :STATUS_ALREADY_DONE::ARCHIVE_JOB_STATUS";
    auto stmt = txn.getConn().createStmt(sql);
    stmt.bindString(":STATUS_ALREADY_DONE", to_string(ArchiveJobStatus::ReadyForDeletion));
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbSelectRepack);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      groupKeys.emplace(rset.columnUint64NoOpt("REPACK_REQUEST_ID"), rset.columnUint64NoOpt("ARCHIVE_FILE_ID"));
    }
  }
  if (groupKeys.empty()) {
    txn.setRowCountForTelemetry(nrows);
    return nrows;
  }

  // Lock every touched group's rows, in one round trip, before reading any completion state.
  lockRepackGroupRows(txn, groupKeys);

  std::string sql = R"SQL(
  WITH target_success_multicopy AS (
      SELECT JOB_ID, REPACK_REQUEST_ID, ARCHIVE_FILE_ID, STATUS, REQUEST_JOB_COUNT
      FROM REPACK_ARCHIVE_ACTIVE_QUEUE
      WHERE REQUEST_JOB_COUNT > 1 AND STATUS <> :STATUS_ALREADY_DONE::ARCHIVE_JOB_STATUS AND JOB_ID IN (
  )SQL";
  sql += sqlpart;
  sql += R"SQL( )
  ),
  locked_siblings AS (
    SELECT DISTINCT aj.JOB_ID, aj.REPACK_REQUEST_ID, aj.ARCHIVE_FILE_ID, aj.STATUS, aj.REQUEST_JOB_COUNT
     FROM REPACK_ARCHIVE_ACTIVE_QUEUE aj
     JOIN target_success_multicopy t USING (REPACK_REQUEST_ID, ARCHIVE_FILE_ID)
     WHERE aj.JOB_ID <> t.JOB_ID AND aj.STATUS = :STATUS_COND_REPLICAS::ARCHIVE_JOB_STATUS
  ),
  ready_for_deletion AS (
    SELECT combined.REPACK_REQUEST_ID, combined.ARCHIVE_FILE_ID
    FROM (
      SELECT t.JOB_ID, t.REPACK_REQUEST_ID, t.ARCHIVE_FILE_ID, t.STATUS, t.REQUEST_JOB_COUNT
      FROM target_success_multicopy t

      UNION ALL

      SELECT ls.JOB_ID, ls.REPACK_REQUEST_ID, ls.ARCHIVE_FILE_ID, ls.STATUS, ls.REQUEST_JOB_COUNT
      FROM locked_siblings ls
     ) AS combined
     GROUP BY combined.REPACK_REQUEST_ID, combined.ARCHIVE_FILE_ID
     HAVING COUNT(DISTINCT combined.JOB_ID) >= MAX(combined.REQUEST_JOB_COUNT)
   )
  UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE aj2
  SET STATUS = CASE
      WHEN rfd.ARCHIVE_FILE_ID IS NOT NULL
        THEN :STATUS_READY_FOR_DELETION::ARCHIVE_JOB_STATUS
      ELSE :STATUS_SUCCESS::ARCHIVE_JOB_STATUS
  END
  FROM target_success_multicopy tsm
        LEFT JOIN ready_for_deletion rfd
            ON rfd.ARCHIVE_FILE_ID = tsm.ARCHIVE_FILE_ID
           AND rfd.REPACK_REQUEST_ID = tsm.REPACK_REQUEST_ID
  WHERE aj2.ARCHIVE_FILE_ID = tsm.ARCHIVE_FILE_ID
    AND aj2.REPACK_REQUEST_ID = tsm.REPACK_REQUEST_ID
    AND (aj2.JOB_ID = tsm.JOB_ID OR aj2.STATUS = :STATUS_COND_REPLICAS2::ARCHIVE_JOB_STATUS);
  )SQL";
  auto stmt1 = txn.getConn().createStmt(sql);
  stmt1.bindString(":STATUS_ALREADY_DONE", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt1.bindString(":STATUS_COND_REPLICAS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt1.bindString(":STATUS_COND_REPLICAS2", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt1.bindString(":STATUS_SUCCESS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt1.bindString(":STATUS_READY_FOR_DELETION", to_string(ArchiveJobStatus::ReadyForDeletion));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateRepackArchiveSuccess);
  stmt1.executeNonQuery();
  nrows += stmt1.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
};

rdbms::Rset ArchiveJobQueueRow::getNextSuccessfulArchiveRepackReportBatch(Transaction& txn, const size_t limit) {
  std::string sql = R"SQL(
        SELECT SRC_URL, JOB_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE
            WHERE STATUS = :STATUS
                ORDER BY PRIORITY DESC, JOB_ID
                LIMIT :LIMIT FOR UPDATE SKIP LOCKED
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt.bindUint32(":LIMIT", static_cast<uint32_t>(limit));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbSelectRepackArchiveToReport);
  auto rset = stmt.executeQuery();
  return rset;
}

rdbms::Rset ArchiveJobQueueRow::deleteSuccessfulRepackArchiveJobBatch(Transaction& txn,
                                                                      std::vector<std::string>& jobIDs) {
  std::string sql = R"SQL(
    WITH DELETED_ROWS AS (
        DELETE FROM REPACK_ARCHIVE_ACTIVE_QUEUE
  )SQL";
  if (!jobIDs.empty()) {
    std::string sqlpart;
    for (const auto& jid : jobIDs) {
      sqlpart += jid + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    sql += std::string("WHERE JOB_ID IN (") + sqlpart + std::string(")");
  } else {
    rdbms::Rset rset;
    return rset;
  }
  sql += R"SQL(
        RETURNING REPACK_REQUEST_ID, VID, SIZE_IN_BYTES
    )
    SELECT
        REPACK_REQUEST_ID, VID,
        COUNT(*) AS ARCHIVED_COUNT,
        COALESCE(SUM(SIZE_IN_BYTES), 0) AS ARCHIVED_BYTES
    FROM DELETED_ROWS
    GROUP BY REPACK_REQUEST_ID, VID;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteRepackArchive);
  auto rset = stmt.executeQuery();
  return rset;
}

void ArchiveJobQueueRow::updateJobRowFailureLog(const std::string& reason, bool is_report_log) {
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() + " " + reason;

  if (auto& logField = is_report_log ? reportFailureLogs : failureLogs; logField.has_value()) {
    logField.value() += failureLog;
  } else {
    logField.emplace(failureLog);
  }
  if (is_report_log) {
    ++totalReportRetries;
  }
};

void ArchiveJobQueueRow::updateRetryCounts(uint64_t mountId) {
  if (lastMountWithFailure == mountId) {
    retriesWithinMount += 1;
  } else {
    retriesWithinMount = 1;
    lastMountWithFailure = mountId;
  }
  totalRetries += 1;
};

uint64_t ArchiveJobQueueRow::requeueJobBatch(Transaction& txn,
                                             ArchiveJobStatus newStatus,
                                             const std::list<std::string>& jobIDs,
                                             bool isRepack) {
  std::string repack_prefix = isRepack ? "REPACK_" : "";
  std::string sql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM
  )SQL";
  sql += repack_prefix + "ARCHIVE_ACTIVE_QUEUE ";
  if (!jobIDs.empty()) {
    std::string sqlpart;
    for (const auto& jid : jobIDs) {
      sqlpart += jid + ",";
    }
    if (!sqlpart.empty()) {
      sqlpart.pop_back();
    }
    sql += std::string("WHERE JOB_ID IN (") + sqlpart + std::string(")");
  } else {
    return 0;
  }
  // Guarded on STATUS still being the active-transfer status this row was dispatched in - same
  // reasoning as failTransferBatchRequeue()'s guard (see that function's comment). The callers
  // here (TapeWriteSingleThread::requeueUnprocessedTasks() after a session-level abort,
  // MigrationReportPacker::ReportLastBatchError::execute()) build jobIDs from in-memory job
  // objects that may be stale by the time this runs: a multi-copy sibling could have exceeded its
  // own retries in the meantime and have already collaterally settled this exact row via
  // failTransferBatchExceedRetries()'s ARCHIVE_REQUEST_ID-wide UPDATE. Without this guard, this
  // DELETE would still match the row by JOB_ID alone and silently resurrect an already-failed (or
  // parked) copy back to AJS_ToTransferFor{User,Repack} in ARCHIVE_PENDING_QUEUE, undoing the
  // sibling's settlement.
  sql += " AND STATUS = :ACTIVE_STATUS::ARCHIVE_JOB_STATUS";
  sql += R"SQL(
        RETURNING *
    )
    INSERT INTO
  )SQL";
  sql += repack_prefix + "ARCHIVE_PENDING_QUEUE ( ";
  if (isRepack) {
    sql += "REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
    JOB_ID,
    ARCHIVE_REQUEST_ID,
    REQUEST_JOB_COUNT,
    TAPE_POOL,
    MOUNT_POLICY,
    PRIORITY,
    MIN_ARCHIVE_REQUEST_AGE,
    ARCHIVE_FILE_ID,
    SIZE_IN_BYTES,
    COPY_NB,
    START_TIME,
    CHECKSUMBLOB,
    CREATION_TIME,
    DISK_INSTANCE,
    DISK_FILE_ID,
    DISK_FILE_OWNER_UID,
    DISK_FILE_GID,
    DISK_FILE_PATH,
    ARCHIVE_REPORT_URL,
    ARCHIVE_ERROR_REPORT_URL,
    REQUESTER_NAME,
    REQUESTER_GROUP,
    SRC_URL,
    STORAGE_CLASS,
    RETRIES_WITHIN_MOUNT,
    MAX_RETRIES_WITHIN_MOUNT,
    TOTAL_RETRIES,
    MAX_TOTAL_RETRIES,
    TOTAL_REPORT_RETRIES,
    MAX_REPORT_RETRIES,
    LAST_MOUNT_WITH_FAILURE,
    VID,
    DRIVE,
    HOST,
    MOUNT_TYPE,
    LOGICAL_LIBRARY,
    STATUS,
    MOUNT_ID,
    FAILURE_LOG)
        SELECT
  )SQL";
  if (isRepack) {
    sql += " M.REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
            M.JOB_ID,
            M.ARCHIVE_REQUEST_ID,
            M.REQUEST_JOB_COUNT,
            M.TAPE_POOL,
            M.MOUNT_POLICY,
            M.PRIORITY,
            M.MIN_ARCHIVE_REQUEST_AGE,
            M.ARCHIVE_FILE_ID,
            M.SIZE_IN_BYTES,
            M.COPY_NB,
            M.START_TIME,
            M.CHECKSUMBLOB,
            M.CREATION_TIME,
            M.DISK_INSTANCE,
            M.DISK_FILE_ID,
            M.DISK_FILE_OWNER_UID,
            M.DISK_FILE_GID,
            M.DISK_FILE_PATH,
            M.ARCHIVE_REPORT_URL,
            M.ARCHIVE_ERROR_REPORT_URL,
            M.REQUESTER_NAME,
            M.REQUESTER_GROUP,
            M.SRC_URL,
            M.STORAGE_CLASS,
            M.RETRIES_WITHIN_MOUNT,
            M.MAX_RETRIES_WITHIN_MOUNT,
            M.TOTAL_RETRIES,
            M.MAX_TOTAL_RETRIES,
            M.TOTAL_REPORT_RETRIES,
            M.MAX_REPORT_RETRIES,
            M.LAST_MOUNT_WITH_FAILURE,
            M.VID,
            M.DRIVE,
            M.HOST,
            M.MOUNT_TYPE,
            M.LOGICAL_LIBRARY,
            :STATUS AS STATUS,
            NULL AS MOUNT_ID,
            FAILURE_LOG || :FAILURE_LOG AS FAILURE_LOG
        FROM MOVED_ROWS M;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(newStatus));
  // Same value as :STATUS (the row must already be at the status it's about to be rewritten to -
  // see the guard comment above) - bound separately per this file's convention of one placeholder
  // name per textual occurrence rather than reusing a name across multiple spots in one statement.
  stmt.bindString(":ACTIVE_STATUS", to_string(newStatus));
  stmt.bindString(":FAILURE_LOG", "UNPROCESSED_TASK_QUEUE_JOB_REQUEUED");
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveBackToPending);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

rdbms::Rset ArchiveJobQueueRow::moveFailedRepackJobBatchToFailedQueueTable(Transaction& txn, uint64_t limit) {
  /* A multi-copy request's failed copy cannot simply be moved on its own: a sibling copy may
   * still be mid-transfer and needs the shared source file (deleted by the caller once this
   * returns) to still exist. So a multi-copy candidate's whole group (see below) is only moved
   * once every copy (REQUEST_JOB_COUNT of them) has itself settled - success or failure - exactly
   * mirroring updateRepackJobSuccess()'s own completeness check from the other direction. A
   * surviving successful sibling (STATUS still AJS_ToReportToRepackForSuccess, never flipped -
   * updateRepackJobSuccess() deliberately leaves a failed sibling invisible to its own check, see
   * that function) is swept into REPACK_ARCHIVE_FAILED_QUEUE here too, alongside the copy that
   * actually failed - the request's outcome is "failed" as a whole once any copy is.
   *
   * REQUEST_JOB_COUNT=1 has no siblings at all, so a failed single-copy row is complete - and
   * moved - the instant it exists: single_copy_candidates below is a plain scan/lock/delete with
   * no join or GROUP BY, the same shape as every other single-row move in this file, so the
   * common case never pays for the multi-copy machinery it has no use for.
   *
   * The multi-copy group is (REPACK_REQUEST_ID, ARCHIVE_FILE_ID), not ARCHIVE_FILE_ID alone. The
   * same file can legitimately be in two repack requests at once - two tapes each holding one of
   * its copies, both being repacked - and those rows are not siblings of each other: each request
   * has its own buffer copy, its own completeness, and its own statistics. With bare
   * ARCHIVE_FILE_ID there a one-copy request whose copy had already succeeded would be counted
   * into the other request's group, swept into REPACK_ARCHIVE_FAILED_QUEUE as a failure, and have
   * its buffer file deleted - all off the back of an unrelated repack's failure.
   *
   * Multi-copy group eligibility is determined in two passes, specifically so that :LIMIT2 cannot
   * hide a ready group behind a backlog of unrelated, still-incomplete ones:
   *  - multi_copy_failed/unlocked_siblings/eligible_multi_groups read every currently-failed
   *    multi-copy row and its siblings, unbounded by :LIMIT2, with no locks taken - a group's
   *    completeness must not depend on whether its own failed row happens to rank inside one
   *    call's PRIORITY/JOB_ID window, or a ready group stuck behind older incomplete ones would
   *    never be reached by a bounded scan (every call would re-examine the same stale candidates
   *    first and never get further). This pass is read-only and therefore cannot deadlock or
   *    block on another transaction's lock; its cost is bounded by how many multi-copy failures
   *    are simultaneously outstanding, not by overall system throughput.
   *  - :LIMIT2 is applied only afterwards, to how many of the now-known-eligible groups this call
   *    actually locks and moves (limited_multi_groups) - keeping the expensive, lock-taking part
   *    bounded exactly as before.
   *  - locked_multi_siblings/complete_multi_groups then re-locks just those (already small)
   *    groups and recomputes completeness from what is actually locked: a concurrent reporter may
   *    hold one of these rows (SKIP LOCKED drops it from the count here), making a group that
   *    looked complete in the unlocked pass above incomplete again.
   *
   * Every lock taken here (single_copy_candidates and locked_multi_siblings) is SKIP LOCKED,
   * deliberately. A blocking lock would deadlock: with both copies of one file failed and two
   * reporters running, SKIP LOCKED hands reporter A copy 1 and reporter B copy 2, then each would
   * block on the row the other holds and PostgreSQL would abort one with 40P01, dropping the
   * whole batch. Skipping instead cannot deadlock - no reporter ever waits - and it fails in the
   * safe direction: a sibling locked by someone else drops out of the count, the group reads as
   * incomplete, and the move is deferred rather than done early. Whichever reporter next finds the
   * group uncontended completes it. The lost "authoritative view" is no loss: this check may only
   * ever err towards waiting, never towards moving.
   *
   * The count is only ever right because transformJobBatchToArchive() guarantees
   * REQUEST_JOB_COUNT == the number of archive rows it inserts for the file (see that function); a
   * count short of the real group would let this move - and the caller's disk-source deletion -
   * fire while a copy was still being written.
   */
  std::string sql = R"SQL(
    WITH single_copy_candidates AS (
        SELECT JOB_ID, REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE
        WHERE STATUS = :STATUS_FAILURE::ARCHIVE_JOB_STATUS AND REQUEST_JOB_COUNT = 1
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT
        FOR UPDATE SKIP LOCKED
    ),
    multi_copy_failed AS (
        SELECT JOB_ID, REPACK_REQUEST_ID, ARCHIVE_FILE_ID, PRIORITY
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE
        WHERE STATUS = :STATUS_FAILURE2::ARCHIVE_JOB_STATUS AND REQUEST_JOB_COUNT > 1
    ),
    unlocked_siblings AS (
        SELECT aj.REPACK_REQUEST_ID, aj.ARCHIVE_FILE_ID, aj.REQUEST_JOB_COUNT
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE aj
        WHERE EXISTS (
            SELECT 1 FROM multi_copy_failed mcf
            WHERE mcf.ARCHIVE_FILE_ID = aj.ARCHIVE_FILE_ID
              AND mcf.REPACK_REQUEST_ID = aj.REPACK_REQUEST_ID
          )
          AND aj.STATUS IN (:STATUS_SUCCESS::ARCHIVE_JOB_STATUS, :STATUS_FAILURE3::ARCHIVE_JOB_STATUS)
    ),
    eligible_multi_groups AS (
        SELECT REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        FROM unlocked_siblings
        GROUP BY REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        HAVING COUNT(*) >= MAX(REQUEST_JOB_COUNT)
    ),
    limited_multi_groups AS (
        SELECT g.REPACK_REQUEST_ID, g.ARCHIVE_FILE_ID
        FROM eligible_multi_groups g
        JOIN multi_copy_failed mcf
          ON mcf.REPACK_REQUEST_ID = g.REPACK_REQUEST_ID AND mcf.ARCHIVE_FILE_ID = g.ARCHIVE_FILE_ID
        GROUP BY g.REPACK_REQUEST_ID, g.ARCHIVE_FILE_ID
        ORDER BY MAX(mcf.PRIORITY) DESC, MIN(mcf.JOB_ID)
        LIMIT :LIMIT2
    ),
    locked_multi_siblings AS (
        SELECT aj.JOB_ID, aj.REPACK_REQUEST_ID, aj.ARCHIVE_FILE_ID, aj.REQUEST_JOB_COUNT
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE aj
        WHERE EXISTS (
            SELECT 1 FROM limited_multi_groups g
            WHERE g.ARCHIVE_FILE_ID = aj.ARCHIVE_FILE_ID
              AND g.REPACK_REQUEST_ID = aj.REPACK_REQUEST_ID
          )
          AND aj.STATUS IN (:STATUS_SUCCESS2::ARCHIVE_JOB_STATUS, :STATUS_FAILURE4::ARCHIVE_JOB_STATUS)
        FOR UPDATE SKIP LOCKED
    ),
    complete_multi_groups AS (
        SELECT REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        FROM locked_multi_siblings
        GROUP BY REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        HAVING COUNT(*) >= MAX(REQUEST_JOB_COUNT)
    ),
    -- The delete set as explicit JOB_IDs, so the DELETE is a plain PK join. An OR of two
    -- EXISTS probes here instead forces a full scan of REPACK_ARCHIVE_ACTIVE_QUEUE per sweep
    -- call. A complete group's rows are by definition all settled and locked, so
    -- locked_multi_siblings already holds exactly the rows to move.
    doomed AS (
        SELECT JOB_ID FROM single_copy_candidates
        UNION ALL
        SELECT ls.JOB_ID
        FROM locked_multi_siblings ls
        JOIN complete_multi_groups g
          ON g.REPACK_REQUEST_ID = ls.REPACK_REQUEST_ID AND g.ARCHIVE_FILE_ID = ls.ARCHIVE_FILE_ID
    ),
    MOVED_ROWS AS (
        DELETE FROM REPACK_ARCHIVE_ACTIVE_QUEUE aq
        USING doomed d
        WHERE aq.JOB_ID = d.JOB_ID
        RETURNING aq.*
    )
    INSERT INTO REPACK_ARCHIVE_FAILED_QUEUE
        SELECT * FROM MOVED_ROWS
        RETURNING REPACK_REQUEST_ID, SIZE_IN_BYTES, SRC_URL, STATUS, VID
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  // :LIMIT and :LIMIT2 are the same batch size, bound separately per this file's convention of
  // one placeholder name per textual occurrence - they bound two independent, unrelated things
  // (single-copy rows moved directly vs. multi-copy groups locked after an unbounded eligibility
  // pass), not one shared budget split between them.
  stmt.bindUint64(":LIMIT", limit);
  stmt.bindUint64(":LIMIT2", limit);
  stmt.bindString(":STATUS_FAILURE", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_FAILURE2", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_FAILURE3", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_FAILURE4", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_SUCCESS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  stmt.bindString(":STATUS_SUCCESS2", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveRepackArchiveToFailed);
  auto rset = stmt.executeQuery();
  return rset;
}

rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction& txn,
                                                          std::list<ArchiveJobStatus> statusList,
                                                          uint64_t limit) {
  /* Every row whose own STATUS is one of statusList (ToReportToUserForSuccess/ForFailure) and not
   * already claimed for reporting is picked up here, multi-copy or not, with no completeness check
   * of its own. This plain filter depends entirely on a guarantee the write side provides:
   * failTransferBatchExceedRetries() parks a failing copy at
   * AJS_WaitReplicasBeforeReportingFailureToDisk (instead of going straight to
   * AJS_ToReportToUserForFailure) whenever a sibling has not been dispatched into
   * ARCHIVE_ACTIVE_QUEUE yet, and updateMultiCopyJobSuccess() does the mirror check on the success
   * side - see both functions. Either one, whichever turns out to be the last copy of the group to
   * settle, promotes every row sharing the ARCHIVE_REQUEST_ID to its final terminal status in the
   * same statement. So a row only ever reaches AJS_ToReportToUserForSuccess/ForFailure once the
   * whole group already has - a copy that reached a terminal status while a sibling was still
   * undispatched would otherwise be reported and deleted here before that sibling had a chance to
   * be accounted for. This query must not re-derive completeness itself: doing so would add a
   * second place that completeness could be computed differently from the write side, racing with
   * it instead of simply running after it.
   *
   * Single-copy rows (REQUEST_JOB_COUNT = 1) never pass through either wait status and are
   * released immediately.
   */
  std::string sql = R"SQL(
      WITH candidates AS (
        SELECT JOB_ID FROM ARCHIVE_ACTIVE_QUEUE
        WHERE STATUS = ANY(ARRAY[
    )SQL";
  // we can move this to new bindArray method for stmt
  std::vector<std::string> statusVec;
  std::vector<std::string> placeholderVec;
  size_t j = 1;
  for (const auto& jstatus : statusList) {
    statusVec.emplace_back(to_string(jstatus));
    std::string plch = std::string(":STATUS") + std::to_string(j);
    placeholderVec.emplace_back(plch);
    sql += plch;
    if (&jstatus != &statusList.back()) {
      sql += std::string(",");
    }
    j++;
  }
  sql += R"SQL(
        ]::ARCHIVE_JOB_STATUS[]) AND IS_REPORTING IS FALSE
        LIMIT :LIMIT FOR UPDATE SKIP LOCKED)
      UPDATE ARCHIVE_ACTIVE_QUEUE SET
        IS_REPORTING = TRUE,
        LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      FROM candidates
      WHERE ARCHIVE_ACTIVE_QUEUE.JOB_ID = candidates.JOB_ID
      RETURNING ARCHIVE_ACTIVE_QUEUE.*
    )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  // we can move the array binding to new bindArray method for STMT
  size_t sz = statusVec.size();
  for (size_t i = 0; i < sz; ++i) {
    stmt.bindString(placeholderVec[i], statusVec[i]);
  }
  stmt.bindUint64(":LIMIT", limit);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  return stmt.executeQuery();
}

uint64_t ArchiveJobQueueRow::getNextArchiveRequestID(rdbms::Conn& conn) {
  const char* const sql = R"SQL(
        SELECT NEXTVAL('ARCHIVE_REQUEST_ID_SEQ') AS ARCHIVE_REQUEST_ID
      )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("ARCHIVE_REQUEST_ID");
}

uint64_t
ArchiveJobQueueRow::cancelArchiveJob(Transaction& txn, const std::string& diskInstance, uint64_t archiveFileID) {
  /* All jobs which were already picked up by the
   * mount to the task queue will run and not be deleted.
   * Deletes only from ARCHIVE_PENDING_QUEUE
   */
  std::string sqlActive = R"SQL(
    DELETE FROM ARCHIVE_PENDING_QUEUE
    WHERE
      DISK_INSTANCE = :DISK_INSTANCE AND
      ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sqlActive);
  stmt.bindString(":DISK_INSTANCE", diskInstance);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileID);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::deleteFailedArchiveJob(Transaction& txn, uint64_t jobID, bool repack) {
  std::string tableName = "ARCHIVE_FAILED_QUEUE";
  if (repack) {
    tableName = "REPACK_ARCHIVE_FAILED_QUEUE";
  }
  std::string sqlActive = R"SQL(
    DELETE FROM
 )SQL";
  sqlActive += tableName;
  sqlActive += R"SQL(
    WHERE
      JOB_ID = :JOB_ID
  )SQL";
  auto stmt = txn.getConn().createStmt(sqlActive);
  stmt.bindUint64(":JOB_ID", jobID);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbDeleteArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

// The SQL below selects d.failure_log AS FAILURE_LOG with a plain reference, no SQL-side
// concatenation: d.failure_log (bound from row->failureLogs.value_or("")) already contains
// m.FAILURE_LOG's old text plus the new entry, since updateJobRowFailureLog() appends onto a
// field that was itself loaded from this same column - concatenating again here would
// duplicate the old text.
uint64_t ArchiveJobQueueRow::failTransferBatchRequeue(Transaction& txn,
                                                      const std::vector<ArchiveJobQueueRow*>& jobs,
                                                      bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";
  const std::string pending_status = isRepack ? to_string(ArchiveJobStatus::AJS_ToTransferForRepack) :
                                                to_string(ArchiveJobStatus::AJS_ToTransferForUser);

  // Build per-row VALUES CTE with numbered bind params
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TR" + s + "::SMALLINT, :RWM" + s + "::SMALLINT, :LMF" + s + "::BIGINT, :FL"
                   + s + "::TEXT, :KM" + s + "::BOOL)";
  }

  std::string sql = R"SQL(
  WITH per_job(job_id, total_retries, retries_within_mount, last_mount, failure_log, keep_mount) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  ),
  moved AS (
    DELETE FROM )SQL";
  // Guarded on STATUS still being the active-transfer status this row was fetched in: within one
  // failTransferBatch() call, a sibling with the SAME ARCHIVE_REQUEST_ID can independently land in
  // the exceed-retries bucket and already have moved this very row to a terminal/report status
  // (failTransferBatchExceedRetries() matches whole multi-copy USER requests by
  // ARCHIVE_REQUEST_ID, not just JOB_ID - see that function). Without this guard, this DELETE would
  // still match the row by JOB_ID alone and silently requeue it back to pending, undoing the
  // sibling's terminal failure for it in the same transaction. The same guard also protects against
  // the equivalent race across two concurrent mounts/transactions.
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE
    WHERE JOB_ID IN (SELECT job_id FROM per_job) AND STATUS = :ACTIVE_STATUS::ARCHIVE_JOB_STATUS
    RETURNING *
  ),
  updated_last_fetch AS (
    INSERT INTO MOUNT_QUEUE_LAST_FETCH (MOUNT_ID, LAST_UPDATE_TIME, QUEUE_TYPE)
      SELECT DISTINCT m.MOUNT_ID,
        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT,
        :QUEUE_TYPE
      FROM moved m
      JOIN per_job d ON m.JOB_ID = d.job_id
      WHERE m.MOUNT_ID IS NOT NULL AND d.keep_mount
    ON CONFLICT (MOUNT_ID, QUEUE_TYPE) DO UPDATE SET
      LAST_UPDATE_TIME = EXCLUDED.LAST_UPDATE_TIME
      WHERE MOUNT_QUEUE_LAST_FETCH.LAST_UPDATE_TIME < EXCLUDED.LAST_UPDATE_TIME - 5
  )
  INSERT INTO )SQL";
  sql += repack_prefix + "ARCHIVE_PENDING_QUEUE (";
  if (isRepack) {
    sql += "\n  REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
  JOB_ID, ARCHIVE_REQUEST_ID, REQUEST_JOB_COUNT, TAPE_POOL, MOUNT_POLICY, PRIORITY,
  MIN_ARCHIVE_REQUEST_AGE, ARCHIVE_FILE_ID, SIZE_IN_BYTES, COPY_NB, START_TIME,
  CHECKSUMBLOB, CREATION_TIME, DISK_INSTANCE, DISK_FILE_ID, DISK_FILE_OWNER_UID,
  DISK_FILE_GID, DISK_FILE_PATH, ARCHIVE_REPORT_URL, ARCHIVE_ERROR_REPORT_URL,
  REQUESTER_NAME, REQUESTER_GROUP, SRC_URL, STORAGE_CLASS,
  MAX_RETRIES_WITHIN_MOUNT, MAX_TOTAL_RETRIES, TOTAL_REPORT_RETRIES, MAX_REPORT_RETRIES,
  VID, DRIVE, HOST, MOUNT_TYPE, LOGICAL_LIBRARY,
  STATUS, TOTAL_RETRIES, RETRIES_WITHIN_MOUNT, LAST_MOUNT_WITH_FAILURE, FAILURE_LOG, MOUNT_ID
  )
  SELECT )SQL";
  if (isRepack) {
    sql += "\n  m.REPACK_REQUEST_ID,";
  }
  sql += R"SQL(
  m.JOB_ID, m.ARCHIVE_REQUEST_ID, m.REQUEST_JOB_COUNT, m.TAPE_POOL, m.MOUNT_POLICY, m.PRIORITY,
  m.MIN_ARCHIVE_REQUEST_AGE, m.ARCHIVE_FILE_ID, m.SIZE_IN_BYTES, m.COPY_NB, m.START_TIME,
  m.CHECKSUMBLOB, m.CREATION_TIME, m.DISK_INSTANCE, m.DISK_FILE_ID, m.DISK_FILE_OWNER_UID,
  m.DISK_FILE_GID, m.DISK_FILE_PATH, m.ARCHIVE_REPORT_URL, m.ARCHIVE_ERROR_REPORT_URL,
  m.REQUESTER_NAME, m.REQUESTER_GROUP, m.SRC_URL, m.STORAGE_CLASS,
  m.MAX_RETRIES_WITHIN_MOUNT, m.MAX_TOTAL_RETRIES, m.TOTAL_REPORT_RETRIES, m.MAX_REPORT_RETRIES,
  m.VID, m.DRIVE, m.HOST, m.MOUNT_TYPE, m.LOGICAL_LIBRARY,
  :PENDING_STATUS::ARCHIVE_JOB_STATUS AS STATUS,
  d.total_retries, d.retries_within_mount, d.last_mount,
  d.failure_log AS FAILURE_LOG,
  CASE WHEN d.keep_mount THEN m.MOUNT_ID ELSE NULL END AS MOUNT_ID
  FROM moved m
  JOIN per_job d ON m.JOB_ID = d.job_id
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":QUEUE_TYPE", isRepack ? "REPACK_ARCHIVE_PENDING" : "ARCHIVE_PENDING");
  stmt.bindString(":PENDING_STATUS", pending_status);
  // Same value as :PENDING_STATUS (the active-transfer status is unchanged by this requeue, only
  // the table moves) - bound separately per this file's convention of one placeholder name per
  // textual occurrence rather than reusing a name across multiple spots in one statement.
  stmt.bindString(":ACTIVE_STATUS", pending_status);

  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    // keepMountId: stay on same mount if within-mount retry budget not exhausted. A job left
    // pinned to a mount that then dies is handled by the garbage collector
    // (ArchiveInactiveMountPendingQueueRoutine), which resets MOUNT_ID once it detects the mount
    // is gone - not a concern for this query.
    const bool keepMountId = row->retriesWithinMount < row->maxRetriesWithinMount;
    // Rotating to a new mount resets the within-mount counter. Writing the exhausted value
    // through instead would deny the job its within-mount budget if the same still-running mount
    // re-popped the row (MOUNT_ID is NULL, so it can): updateRetryCounts() only resets the
    // counter when it sees a DIFFERENT mount id, so on the same mount it would increment from the
    // ceiling and immediately rotate again, every time. Reset at bind time rather than on the
    // row: keepMountId above is derived FROM row->retriesWithinMount, so zeroing the member first
    // would make every job look like it still had budget. Retrieve reaches the same result the
    // other way round - RetrieveRdbJob::prepareFailTransfer() zeroes the member and the retrieve
    // SQL then reads retries_within_mount > 0 as its own keep-mount signal.
    const uint32_t retriesWithinMount = keepMountId ? row->retriesWithinMount : 0;
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TR" + s, row->totalRetries);
    stmt.bindUint32(":RWM" + s, retriesWithinMount);
    stmt.bindUint64(":LMF" + s, row->lastMountWithFailure);
    stmt.bindString(":FL" + s, row->failureLogs.value_or(""));
    stmt.bindBool(":KM" + s, keepMountId);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbMoveArchiveBackToPending);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

// The SQL below sets FAILURE_LOG = d.failure_log with a plain assignment, no SQL-side
// concatenation: d.failure_log (bound from row->failureLogs.value_or("")) already has
// a.FAILURE_LOG's old text plus the new entry appended in memory (updateJobRowFailureLog()) -
// concatenating again here would duplicate the old text.
uint64_t ArchiveJobQueueRow::failTransferBatchExceedRetries(Transaction& txn,
                                                            const std::vector<ArchiveJobQueueRow*>& jobs,
                                                            bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";
  const ArchiveJobStatus newStatus =
    isRepack ? ArchiveJobStatus::AJS_ToReportToRepackForFailure : ArchiveJobStatus::AJS_ToReportToUserForFailure;

  // USER multi-copy requests fail together, by ARCHIVE_REQUEST_ID, and a sibling that is still
  // being written is force-settled here immediately rather than left to run to completion:
  // nothing in the user workflow reads "all copies are terminal" as permission to delete
  // anything, so there is no correctness reason to let a doomed transfer finish, only a wasted
  // one.
  //
  // Settling every visible sibling is not the same as settling the WHOLE group.
  // sibling_presence counts how many rows for this ARCHIVE_REQUEST_ID currently exist in
  // ARCHIVE_ACTIVE_QUEUE at all, independently of their own STATUS - a row that has not been
  // dispatched out of ARCHIVE_PENDING_QUEUE yet is invisible to this query and this UPDATE alike,
  // and forcing the visible copies straight to the terminal AJS_ToReportToUserForFailure would let
  // flagReportingJobsByStatus() - which trusts STATUS alone, see that function - report and delete
  // them before the pending copy ever gets a chance to run. So only when present_count already
  // reaches REQUEST_JOB_COUNT (no copy is still pending elsewhere) do the visible rows go straight
  // to the terminal failure status; otherwise they park at
  // AJS_WaitReplicasBeforeReportingFailureToDisk instead - the mirror image of
  // the wait updateMultiCopyJobSuccess() already does on the success side, for the mirror-image
  // reason: a copy that hasn't started yet might still succeed, so the group's fate genuinely is not
  // decided yet. Whichever copy - success or failure - turns out to be the LAST to settle resolves
  // the whole group atomically in its own write (see updateMultiCopyJobSuccess(), which this parked
  // row's eventual sibling will reach): once every copy is accounted for, any parked failure means
  // the group fails, with no further checks needed at report time.
  //
  // REPACK is per-JOB_ID and must NEVER be failed as a group here, even though repack multi-copy
  // rows do carry req_job_count > 1 and a shared ARCHIVE_REQUEST_ID (transformJobBatchToArchive()
  // writes the retrieve request's id into every copy of a file). Repack is exactly the case the
  // paragraph above does not apply to, because repack DOES own a shared source file: the repack
  // buffer copy is deleted by getNextFailedArchiveRepackReportBatch() -> deleteDiskFiles(), gated on
  // moveFailedRepackJobBatchToFailedQueueTable() reporting the group complete - and that
  // completeness is inferred from STATUS alone. Marking a copy another drive is still writing as
  // failed makes that inference false: the buffer would be deleted from under a live reader, and a
  // copy that nevertheless finished would land in the catalogue with no scheduler row left to
  // report it. So a failed repack copy settles alone and the group waits; it is swept up, surviving
  // successes included, only once every copy has reached a terminal status of its own.
  // USER multi-copy groups are settled here by ARCHIVE_REQUEST_ID, the same group key
  // updateMultiCopyJobSuccess() settles by - so this UPDATE's target rows must be locked in the
  // same sorted order that function already locks in (lockArchiveRequestGroupRows()), or a
  // transfer failing here and a sibling copy succeeding there, concurrently, can lock the group's
  // rows in opposite orders and have Postgres abort one of the two as a deadlock victim. The
  // request ids are already known from the input jobs - unlike updateMultiCopyJobSuccess(), which
  // only has job ids and must look them up - so no extra SELECT is needed here.
  if (!isRepack) {
    std::set<uint64_t> requestIds;
    for (const auto* row : jobs) {
      if (row->reqJobCount > 1) {
        requestIds.insert(row->reqId);
      }
    }
    if (!requestIds.empty()) {
      lockArchiveRequestGroupRows(txn, requestIds);
    }
  }

  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TR" + s + "::SMALLINT, :RWM" + s + "::SMALLINT, :LMF" + s + "::BIGINT, :FL"
                   + s + "::TEXT" + ", :REQID" + s + "::BIGINT, :RJC" + s + "::SMALLINT)";
  }

  std::string sql = R"SQL(
  WITH per_job(job_id, total_retries, retries_within_mount, last_mount, failure_log,
               archive_request_id, req_job_count) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  ))SQL";
  if (isRepack) {
    sql += R"SQL(
  UPDATE )SQL";
    sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE a
  SET
    STATUS = :STATUS::ARCHIVE_JOB_STATUS,
    TOTAL_RETRIES = d.total_retries,
    RETRIES_WITHIN_MOUNT = d.retries_within_mount,
    LAST_MOUNT_WITH_FAILURE = d.last_mount,
    FAILURE_LOG = d.failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  WHERE a.JOB_ID = d.job_id
  )SQL";
  } else {
    sql += R"SQL(
  ,
  -- DISTINCT: this batch can itself carry two siblings of the same multi-copy request (e.g. two
  -- copies on different drives both exceeding retries in the same poll), which would otherwise join
  -- per_job to ARCHIVE_ACTIVE_QUEUE once per per_job row sharing that request and double-count.
  sibling_presence AS (
    SELECT d.archive_request_id AS archive_request_id, COUNT(DISTINCT aj.JOB_ID) AS present_count
    FROM per_job d
    JOIN ARCHIVE_ACTIVE_QUEUE aj ON aj.ARCHIVE_REQUEST_ID = d.archive_request_id
    WHERE d.req_job_count > 1
    GROUP BY d.archive_request_id
  )
  UPDATE ARCHIVE_ACTIVE_QUEUE a
  SET
    STATUS = CASE
        WHEN d.req_job_count = 1 THEN :STATUS_TERMINAL_FAILURE::ARCHIVE_JOB_STATUS
        WHEN sp.present_count >= d.req_job_count THEN :STATUS_TERMINAL_FAILURE2::ARCHIVE_JOB_STATUS
        ELSE :STATUS_WAIT_FAILURE::ARCHIVE_JOB_STATUS
    END,
    TOTAL_RETRIES = d.total_retries,
    RETRIES_WITHIN_MOUNT = d.retries_within_mount,
    LAST_MOUNT_WITH_FAILURE = d.last_mount,
    FAILURE_LOG = d.failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  LEFT JOIN sibling_presence sp ON sp.archive_request_id = d.archive_request_id
  WHERE (d.req_job_count = 1 AND a.JOB_ID = d.job_id)
     OR (d.req_job_count > 1 AND a.ARCHIVE_REQUEST_ID = d.archive_request_id)
  )SQL";
  }

  // Two distinct placeholder names bind the same newStatus value (:STATUS_TERMINAL_FAILURE/2) -
  // the statement parser (ParamNameToIdx) rejects a repeated :name as a duplicate, so each
  // textual occurrence needs its own name, not just a stylistic preference.
  auto stmt = txn.getConn().createStmt(sql);
  if (isRepack) {
    stmt.bindString(":STATUS", to_string(newStatus));
  } else {
    stmt.bindString(":STATUS_TERMINAL_FAILURE", to_string(newStatus));
    stmt.bindString(":STATUS_TERMINAL_FAILURE2", to_string(newStatus));
    stmt.bindString(":STATUS_WAIT_FAILURE", to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingFailureToDisk));
  }

  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TR" + s, row->totalRetries);
    stmt.bindUint32(":RWM" + s, row->retriesWithinMount);
    stmt.bindUint64(":LMF" + s, row->lastMountWithFailure);
    stmt.bindString(":FL" + s, row->failureLogs.value_or(""));
    stmt.bindUint64(":REQID" + s, row->reqId);
    stmt.bindUint32(":RJC" + s, row->reqJobCount);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbUpdateArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::failReportBatchExceedRetries(Transaction& txn,
                                                          const std::vector<ArchiveJobQueueRow*>& jobs,
                                                          bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }

  // USER multi-copy requests move all copies atomically, by ARCHIVE_REQUEST_ID. Repack keys on
  // JOB_ID alone: its
  // multi-copy grouping is by (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) and is handled entirely inside
  // updateRepackJobSuccess() below. Note that repack rows do NOT always have req_job_count = 1 -
  // transformJobBatchToArchive() sets it to the number of copies the file is being re-archived to -
  // so the repack branch must not reuse the user branch's req_job_count test. archive_request_id
  // and req_job_count stay in the VALUES CTE for both branches so the per-row shape is identical;
  // only the user branch's WHERE reads them.
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TRR" + s + "::SMALLINT, :RFL" + s + "::TEXT, :REQID" + s + "::BIGINT, :RJC"
                   + s + "::SMALLINT)";
  }

  if (isRepack) {
    // REPACK: "exceeded report retries" here means the DB WRITE failed repeatedly, not that the
    // job failed - repack transfer failures never reach this function
    // (failTransferBatchExceedRetries() handles those). The only way here is
    // updateRepackJobSuccess() itself failing to persist an already-successful transfer, so this
    // means "recover that lost write", not "report a failure".
    // STATUS is intentionally NOT set by the UPDATE below: only updateRepackJobSuccess() knows
    // whether a multi-copy request's other copies have also succeeded - moving straight to
    // ARCHIVE_FAILED_QUEUE here (as the non-repack path below does) would let
    // getNextSuccessfulArchiveRepackReportBatch() delete the source file before every copy exists.
    //
    // The group lock below MUST run before the retry-metadata UPDATE that follows it, not after.
    // This recovery path's own UPDATE takes a row lock on exactly its own batch's JOB_IDs,
    // unsorted relative to the rest of the group; updateRepackJobSuccess(), called right after,
    // takes its group lock (lockRepackGroupRows()) in sorted order. A concurrent, ordinary
    // updateRepackJobSuccess() call completing a DIFFERENT copy of the SAME group always starts
    // with that same sorted group lock. If this recovery call locked its own row first, it could
    // hold a higher-sorted JOB_ID while the concurrent call holds a lower one and waits for this
    // one's group lock to reach it - a lock-order cycle, and Postgres resolves cycles by aborting
    // one side (error 40P01). Taking the group lock here first makes recovery follow the exact
    // same lock-acquisition order as every other caller, so no cycle can form. Locking rows this
    // statement will touch again moments later via updateRepackJobSuccess() is harmless - a
    // transaction re-locking a row it already holds is a no-op.
    std::set<std::pair<uint64_t, uint64_t>> groupKeys;
    for (const auto* row : jobs) {
      if (row->reqJobCount > 1) {
        groupKeys.emplace(row->repackRequestId, row->archiveFileID);
      }
    }
    lockRepackGroupRows(txn, groupKeys);

    std::string sql = R"SQL(
    WITH per_job(job_id, total_report_retries, report_failure_log,
                 archive_request_id, req_job_count) AS (
      VALUES )SQL";
    sql += values_rows;
    sql += R"SQL(
    )
    UPDATE REPACK_ARCHIVE_ACTIVE_QUEUE a SET
      TOTAL_REPORT_RETRIES = d.total_report_retries,
      IS_REPORTING = FALSE,
      REPORT_FAILURE_LOG = d.report_failure_log,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
    FROM per_job d
    WHERE a.JOB_ID = d.job_id
    )SQL";

    auto stmt = txn.getConn().createStmt(sql);
    for (size_t i = 0; i < jobs.size(); ++i) {
      const auto* row = jobs[i];
      const std::string s = std::to_string(i);
      stmt.bindUint64(":JID" + s, row->jobId);
      stmt.bindUint32(":TRR" + s, row->totalReportRetries);
      stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
      stmt.bindUint64(":REQID" + s, row->reqId);
      stmt.bindUint32(":RJC" + s, row->reqJobCount);
    }
    txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
    stmt.executeNonQuery();

    std::vector<std::string> jobIds;
    jobIds.reserve(jobs.size());
    for (const auto* row : jobs) {
      jobIds.push_back(std::to_string(row->jobId));
    }
    return ArchiveJobQueueRow::updateRepackJobSuccess(txn, jobIds);
  }

  // USER: a genuine terminal failure. Statement 1 writes the terminal status and the report
  // counters, expanding to every sibling copy by ARCHIVE_REQUEST_ID for a multi-copy request so
  // the whole request settles atomically. Statement 2 moves exactly the rows statement 1 touched
  // into ARCHIVE_FAILED_QUEUE.
  //
  // These MUST stay two statements. Folding them into one chained CTE
  // (UPDATE ... RETURNING -> INSERT -> DELETE) silently does not work: every sub-statement of a
  // single statement runs under one snapshot and one command id, and PostgreSQL discards a DELETE
  // of a row that same statement has already updated - "only the update is performed", see the
  // notes on data-modifying statements in WITH. The delete scan still sees the pre-update tuple,
  // attempts the delete, gets TM_SelfModified with cmax == es_output_cid, and is skipped without an
  // error. Each row would then be duplicated into ARCHIVE_FAILED_QUEUE and left behind in
  // ARCHIVE_ACTIVE_QUEUE at ReadyForDeletion, where nothing collects it: the ReadyForDeletion sweep
  // (getNextSuccessfulArchiveRepackReportBatch()) only reads REPACK_ARCHIVE_ACTIVE_QUEUE, and the
  // reporting-recycle index is restricted to IS_REPORTING IS TRUE. Worse for a multi-copy request,
  // the stranded rows stay matchable, and updateMultiCopyJobSuccess()'s guard excludes only
  // AJS_ToReportToUserForFailure - not ReadyForDeletion - so a later success on another copy would
  // flip them back to AJS_ToReportToUserForSuccess and report success to EOS for a file already
  // filed as failed. Every other move in this file has the same two-step shape for the same reason:
  // the row is modified once, and a DELETE ... RETURNING * leads.
  //
  // sibling_presence mirrors failTransferBatchExceedRetries()'s guard of the same name, for the
  // same reason: a multi-copy request's rows visible here are only the copies already dispatched
  // out of ARCHIVE_PENDING_QUEUE. Setting every visible row straight to ReadyForDeletion -
  // unconditionally, as this used to do - lets statement 2 move the group into ARCHIVE_FAILED_QUEUE
  // before a copy that is still pending even gets a chance to run, orphaning it: nothing ever
  // revisits a row already deleted from ARCHIVE_ACTIVE_QUEUE. So only when present_count already
  // reaches REQUEST_JOB_COUNT do the visible rows go straight to ReadyForDeletion; otherwise they
  // park at AJS_WaitReplicasBeforeReportingFailureToDisk, to be collected once the group's last
  // copy settles.
  //
  // That pending-sibling park must NOT apply once this row's own group outcome was already
  // decided before this call - the a.STATUS branch below, checked first. This function is reached
  // from two callers with different histories: ArchiveMount::failReportBatch()'s write-recovery
  // path (STATUS still AJS_ToTransferForUser - completeness genuinely was never determined, the
  // park above is correct) and RelationalDB::failArchiveJobBatchReport()'s EOS-notification-retry
  // path, fed only by getNextArchiveJobsToReportBatch() (STATUS already
  // AJS_ToReportToUserForSuccess/ForFailure - the group's fate was already decided, atomically,
  // by whichever settlement call got there last; a missing sibling row here means it was already
  // reported and deleted, not that it is still pending). Applying the park to the second caller
  // would wait forever on a sibling that will never return: nothing revisits an
  // AJS_WaitReplicasBeforeReportingFailureToDisk row once the one event that could complete its
  // group (the sibling settling) already happened and that sibling's row is gone.
  std::string sql = R"SQL(
  WITH per_job(job_id, total_report_retries, report_failure_log,
               archive_request_id, req_job_count) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  ),
  -- DISTINCT: this batch can itself carry two siblings of the same multi-copy request, which would
  -- otherwise join per_job to ARCHIVE_ACTIVE_QUEUE once per per_job row sharing that request and
  -- double-count.
  sibling_presence AS (
    SELECT d.archive_request_id AS archive_request_id, COUNT(DISTINCT aj.JOB_ID) AS present_count
    FROM per_job d
    JOIN ARCHIVE_ACTIVE_QUEUE aj ON aj.ARCHIVE_REQUEST_ID = d.archive_request_id
    WHERE d.req_job_count > 1
    GROUP BY d.archive_request_id
  )
  UPDATE ARCHIVE_ACTIVE_QUEUE a SET
    STATUS = CASE
        WHEN d.req_job_count = 1 THEN :STATUS_TERMINAL_FAILURE::ARCHIVE_JOB_STATUS
        WHEN a.STATUS = ANY(ARRAY[:STATUS_ALREADY_DECIDED_SUCCESS, :STATUS_ALREADY_DECIDED_FAILURE]
                            ::ARCHIVE_JOB_STATUS[])
          THEN :STATUS_TERMINAL_FAILURE3::ARCHIVE_JOB_STATUS
        WHEN sp.present_count >= d.req_job_count THEN :STATUS_TERMINAL_FAILURE2::ARCHIVE_JOB_STATUS
        ELSE :STATUS_WAIT_FAILURE::ARCHIVE_JOB_STATUS
    END,
    TOTAL_REPORT_RETRIES = d.total_report_retries,
    IS_REPORTING = FALSE,
    REPORT_FAILURE_LOG = d.report_failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  LEFT JOIN sibling_presence sp ON sp.archive_request_id = d.archive_request_id
  WHERE (d.req_job_count = 1 AND a.JOB_ID = d.job_id)
     OR (d.req_job_count > 1 AND a.ARCHIVE_REQUEST_ID = d.archive_request_id)
  RETURNING a.JOB_ID, a.STATUS
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  // Three distinct placeholder names bind the same ReadyForDeletion value - ParamNameToIdx rejects
  // a repeated :name as a duplicate, so each textual occurrence needs its own name.
  stmt.bindString(":STATUS_TERMINAL_FAILURE", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt.bindString(":STATUS_TERMINAL_FAILURE2", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt.bindString(":STATUS_TERMINAL_FAILURE3", to_string(ArchiveJobStatus::ReadyForDeletion));
  stmt.bindString(":STATUS_ALREADY_DECIDED_SUCCESS", to_string(ArchiveJobStatus::AJS_ToReportToUserForSuccess));
  stmt.bindString(":STATUS_ALREADY_DECIDED_FAILURE", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
  stmt.bindString(":STATUS_WAIT_FAILURE", to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingFailureToDisk));
  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TRR" + s, row->totalReportRetries);
    stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
    stmt.bindUint64(":REQID" + s, row->reqId);
    stmt.bindUint32(":RJC" + s, row->reqJobCount);
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
  // JOB_IDs come straight back from the DB, so they are safe to inline in statement 2. The Rset is
  // scoped: executeQuery() runs in single-row mode and marks the connection async-in-progress, and
  // any further statement on that connection throws until the result is exhausted or released.
  // Only rows that actually reached ReadyForDeletion move on to statement 2 - a row parked at
  // AJS_WaitReplicasBeforeReportingFailureToDisk above stays in ARCHIVE_ACTIVE_QUEUE, waiting for
  // its group's last copy to settle.
  std::string settledJobIds;
  uint64_t touchedRows = 0;
  {
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      ++touchedRows;
      if (rset.columnString("STATUS") != to_string(ArchiveJobStatus::ReadyForDeletion)) {
        continue;
      }
      if (!settledJobIds.empty()) {
        settledJobIds += ",";
      }
      settledJobIds += std::to_string(rset.columnUint64("JOB_ID"));
    }
  }
  if (settledJobIds.empty()) {
    // Nothing reached ReadyForDeletion yet - either nothing matched (jobs cancelled or already
    // collected in the meantime) or every touched row is still parked, waiting on a sibling.
    txn.setRowCountForTelemetry(touchedRows);
    return 0;
  }

  std::string moveSql = R"SQL(
    WITH MOVED_ROWS AS (
        DELETE FROM ARCHIVE_ACTIVE_QUEUE
        WHERE JOB_ID IN ()SQL";
  moveSql += settledJobIds;
  moveSql += R"SQL()
        RETURNING *
    ) INSERT INTO ARCHIVE_FAILED_QUEUE SELECT * FROM MOVED_ROWS
  )SQL";
  auto moveStmt = txn.getConn().createStmt(moveSql);
  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
  moveStmt.executeNonQuery();
  auto nrows = moveStmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(touchedRows + nrows);
  return nrows;
}

uint64_t ArchiveJobQueueRow::failReportBatchRequeue(Transaction& txn,
                                                    const std::vector<ArchiveJobQueueRow*>& jobs,
                                                    bool isRepack) {
  if (jobs.empty()) {
    return 0;
  }
  const std::string repack_prefix = isRepack ? "REPACK_" : "";

  // Build per-row VALUES CTE: (job_id, total_report_retries, report_failure_log)
  std::string values_rows;
  for (size_t i = 0; i < jobs.size(); ++i) {
    if (i > 0) {
      values_rows += ",\n    ";
    }
    const std::string s = std::to_string(i);
    values_rows += "(:JID" + s + "::BIGINT, :TRR" + s + "::SMALLINT, :RFL" + s + "::TEXT)";
  }

  // Reset IS_REPORTING so the flagging loop will pick these jobs up for another attempt.
  //
  // STATUS is deliberately left untouched here. This function is reached from two different
  // places with two different STATUS histories:
  //  - Scheduler::reportArchiveJobsBatch()'s EOS/disk-report failure path: the row's STATUS is
  //    already AJS_ToReportToUserForSuccess/ForFailure (that's how getNextArchiveJobsToReportBatch()
  //    found it in the first place) and stays correct, no write needed.
  //  - ArchiveMount::failReportBatch()'s FailedReportMoveToQueue path (the transfer and catalogue
  //    write already succeeded; it's the scheduler-DB's own setJobBatchTransferred() write
  //    recording that fact that failed): by the time this function is reached at all,
  //    cta::ArchiveMount::reportJobsBatchTransferred() has already retried
  //    setJobBatchTransferred() with backoff (see that function) and still failed, so this is the
  //    genuinely-exhausted fallback, not the common case. STATUS stays whatever it was at the
  //    original getNextJobBatch() fetch - AJS_ToTransferFor{User,Repack} - so
  //    getNextArchiveJobsToReportBatch() cannot find the row again; it is picked up later by the
  //    dead-mount GC sweep (RelationalDB::handleInactiveMountActiveQueues()) and requeued for a
  //    new tape transfer instead. This is a known, accepted residual gap for an already-exhausted
  //    retry, not something this function can fix on its own: correctly recovering the row here
  //    (as the retry above does when it succeeds) needs setJobBatchTransferred()'s full
  //    single-copy/multi-copy/repack completion logic, which this generic requeue helper does not
  //    have and should not try to duplicate.
  std::string sql = R"SQL(
  WITH per_job(job_id, total_report_retries, report_failure_log) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  )
  UPDATE )SQL";
  sql += repack_prefix + R"SQL(ARCHIVE_ACTIVE_QUEUE a SET
    IS_REPORTING = FALSE,
    TOTAL_REPORT_RETRIES = d.total_report_retries,
    REPORT_FAILURE_LOG = d.report_failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  WHERE a.JOB_ID = d.job_id
  )SQL";

  auto stmt = txn.getConn().createStmt(sql);
  for (size_t i = 0; i < jobs.size(); ++i) {
    const auto* row = jobs[i];
    const std::string s = std::to_string(i);
    stmt.bindUint64(":JID" + s, row->jobId);
    stmt.bindUint32(":TRR" + s, row->totalReportRetries);
    stmt.bindString(":RFL" + s, row->reportFailureLogs.value_or(""));
  }

  txn.getConn().setDbQuerySummary(cta::semconv::attr::DbQuerySummary::kDbFailReportBatchArchive);
  stmt.executeNonQuery();
  auto nrows = stmt.getNbAffectedRows();
  txn.setRowCountForTelemetry(nrows);
  return nrows;
}

}  // namespace cta::schedulerdb::postgres

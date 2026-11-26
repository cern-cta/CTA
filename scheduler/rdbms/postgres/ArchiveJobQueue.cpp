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
  size_t i = 0;
  for (const auto& [repackRequestId, archiveFileId] : groupKeys) {
    if (i > 0) {
      pairs += ",";
    }
    const std::string s = std::to_string(i);
    pairs += "(:R" + s + "::BIGINT, :F" + s + "::BIGINT)";
    ++i;
  }
  std::string sql = "SELECT JOB_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                    "WHERE (REPACK_REQUEST_ID, ARCHIVE_FILE_ID) IN ("
                    + pairs
                    + ") "
                      "ORDER BY REPACK_REQUEST_ID, ARCHIVE_FILE_ID, JOB_ID FOR UPDATE";
  auto stmt = txn.getConn().createStmt(sql);
  i = 0;
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
   * When the group is complete, the CASE below promotes every still-open row (not just the
   * target(s) - any copy already parked at one of the two wait statuses from an earlier call also
   * matches aj2.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID below) to FAILED if any_failed, else to
   * ready-for-reporting - request-wide outcomes settle together either way, exactly once, in
   * whichever call turns out to be the group's last. Because of that, flagReportingJobsByStatus()
   * never needs to re-derive completeness itself: by the time any row reaches a terminal status,
   * the whole group was already set to a terminal status together, in this one statement - reporting
   * is free to just pick up rows whose STATUS says so, with no race against settlement.
   * aj2.STATUS <> FAILED excludes a row that already independently reached
   * AJS_ToReportToUserForFailure: it is already correctly terminal and does not need re-touching,
   * only counting (which it gets via the UNION ALL above, not via this final UPDATE's WHERE).
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
            AND aj.STATUS = ANY(ARRAY[:STATUS_COND_REPLICAS, :STATUS_COND_REPLICAS_FAILURE, :STATUS_FAILED]
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
            THEN :STATUS_FAILED::ARCHIVE_JOB_STATUS
          WHEN rg.ARCHIVE_REQUEST_ID IS NOT NULL
            THEN :STATUS_READY_FOR_REPORTING::ARCHIVE_JOB_STATUS
          ELSE :STATUS_WAIT_FOR_ALL_BEFORE_REPORT::ARCHIVE_JOB_STATUS
      END,
      LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
      FROM target_success_multicopy tsm
          LEFT JOIN ready_groups rg
              ON rg.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
      WHERE aj2.ARCHIVE_REQUEST_ID = tsm.ARCHIVE_REQUEST_ID
        AND aj2.STATUS <> :STATUS_FAILED::ARCHIVE_JOB_STATUS;
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindString(":STATUS_READY_FOR_REPORTING", to_string(newStatus));
  stmt.bindString(":STATUS_COND_REPLICAS", to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_COND_REPLICAS_FAILURE",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingFailureToDisk));
  stmt.bindString(":STATUS_WAIT_FOR_ALL_BEFORE_REPORT",
                  to_string(ArchiveJobStatus::AJS_WaitReplicasBeforeReportingSuccessToDisk));
  stmt.bindString(":STATUS_FAILED", to_string(ArchiveJobStatus::AJS_ToReportToUserForFailure));
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
   * The final UPDATE is restricted to aj2.JOB_ID IN the targets themselves (always touched,
   * whatever their prior status, since settling them is this call's own job) OR aj2.STATUS
   * already = AJS_ToReportToRepackForSuccess (an already-settled sibling, safe to promote or
   * re-affirm). It must NOT match a sibling purely by (REPACK_REQUEST_ID, ARCHIVE_FILE_ID)
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
   */
  uint64_t nrows = 0;

  // Single-copy jobs: no sibling coordination needed, settle immediately.
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
  std::set<std::pair<uint64_t, uint64_t>> groupKeys;
  {
    std::string sql = "SELECT REPACK_REQUEST_ID, ARCHIVE_FILE_ID FROM REPACK_ARCHIVE_ACTIVE_QUEUE "
                      "WHERE JOB_ID IN ("
                      + sqlpart + ") AND REQUEST_JOB_COUNT > 1";
    auto stmt = txn.getConn().createStmt(sql);
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
      WHERE REQUEST_JOB_COUNT > 1 AND JOB_ID IN (
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
   * returns) to still exist. So this only moves a candidate's whole group (see below) once
   * every copy (REQUEST_JOB_COUNT of them) has itself settled - success or failure - exactly
   * mirroring updateRepackJobSuccess()'s own completeness check from the other direction. A
   * surviving successful sibling (STATUS still AJS_ToReportToRepackForSuccess, never flipped -
   * updateRepackJobSuccess() deliberately leaves a failed sibling invisible to its own check, see
   * that function) is swept into REPACK_ARCHIVE_FAILED_QUEUE here too, alongside the copy that
   * actually failed - the request's outcome is "failed" as a whole once any copy is.
   *
   * The group is (REPACK_REQUEST_ID, ARCHIVE_FILE_ID), not ARCHIVE_FILE_ID alone. The same file can
   * legitimately be in two repack requests at once - two tapes each holding one of its copies, both
   * being repacked - and those rows are not siblings of each other: each request has its own buffer
   * copy, its own completeness, and its own statistics. Note that candidate_failed's JOB_ID does
   * not provide that scoping: it only identifies and locks the candidate ROW. The group identity is
   * whatever settled_siblings, complete_failed_groups and the DELETE key on, and with bare
   * ARCHIVE_FILE_ID there a one-copy request whose copy had already succeeded would be counted into
   * the other request's group, swept into REPACK_ARCHIVE_FAILED_QUEUE as a failure, and have its
   * buffer file deleted - all off the back of an unrelated repack's failure.
   *
   * Both locks are SKIP LOCKED, deliberately. A blocking lock on settled_siblings deadlocks: with
   * both copies of one file failed and two reporters running, SKIP LOCKED hands reporter A copy 1
   * and reporter B copy 2, then each blocks on the row the other holds and PostgreSQL aborts one
   * with 40P01, dropping the whole batch. Skipping instead cannot deadlock - no reporter ever
   * waits - and it fails in the safe direction: a sibling locked by someone else drops out of the
   * count, the group reads as incomplete, and the move is deferred rather than done early. Whichever
   * reporter next finds the group uncontended completes it. The lost "authoritative view" is no
   * loss: this check may only ever err towards waiting, never towards moving.
   *
   * REQUEST_JOB_COUNT=1 has no siblings, so such a row is complete (and moved) the moment it is
   * itself the candidate - no special case needed, the counting degenerates to the same result
   * directly. The count is only ever right because transformJobBatchToArchive() guarantees
   * REQUEST_JOB_COUNT == the number of archive rows it inserts for the file (see that function); a
   * count short of the real group would let this move - and the caller's disk-source deletion -
   * fire while a copy was still being written.
   */
  std::string sql = R"SQL(
    WITH candidate_failed AS (
        SELECT JOB_ID, REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE
        WHERE STATUS = :STATUS_FAILURE::ARCHIVE_JOB_STATUS
        ORDER BY PRIORITY DESC, JOB_ID
        LIMIT :LIMIT
        FOR UPDATE SKIP LOCKED
    ),
    settled_siblings AS (
        SELECT aj.JOB_ID, aj.REPACK_REQUEST_ID, aj.ARCHIVE_FILE_ID, aj.STATUS, aj.REQUEST_JOB_COUNT
        FROM REPACK_ARCHIVE_ACTIVE_QUEUE aj
        WHERE EXISTS (
            SELECT 1 FROM candidate_failed cf
            WHERE cf.ARCHIVE_FILE_ID = aj.ARCHIVE_FILE_ID
              AND cf.REPACK_REQUEST_ID = aj.REPACK_REQUEST_ID
          )
          AND aj.STATUS IN (:STATUS_SUCCESS::ARCHIVE_JOB_STATUS, :STATUS_FAILURE2::ARCHIVE_JOB_STATUS)
        FOR UPDATE SKIP LOCKED
    ),
    complete_failed_groups AS (
        SELECT REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        FROM settled_siblings
        GROUP BY REPACK_REQUEST_ID, ARCHIVE_FILE_ID
        HAVING COUNT(*) >= MAX(REQUEST_JOB_COUNT)
    ),
    MOVED_ROWS AS (
        DELETE FROM REPACK_ARCHIVE_ACTIVE_QUEUE aq
        WHERE EXISTS (
            SELECT 1 FROM complete_failed_groups g
            WHERE g.ARCHIVE_FILE_ID = aq.ARCHIVE_FILE_ID
              AND g.REPACK_REQUEST_ID = aq.REPACK_REQUEST_ID
          )
        RETURNING *
    )
    INSERT INTO REPACK_ARCHIVE_FAILED_QUEUE
        SELECT * FROM MOVED_ROWS
        RETURNING REPACK_REQUEST_ID, SIZE_IN_BYTES, SRC_URL, STATUS
  )SQL";
  auto stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":LIMIT", limit);
  stmt.bindString(":STATUS_FAILURE", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_FAILURE2", to_string(ArchiveJobStatus::AJS_ToReportToRepackForFailure));
  stmt.bindString(":STATUS_SUCCESS", to_string(ArchiveJobStatus::AJS_ToReportToRepackForSuccess));
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
        WHEN sp.present_count >= d.req_job_count THEN :STATUS_TERMINAL_FAILURE::ARCHIVE_JOB_STATUS
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

  auto stmt = txn.getConn().createStmt(sql);
  if (isRepack) {
    stmt.bindString(":STATUS", to_string(newStatus));
  } else {
    stmt.bindString(":STATUS_TERMINAL_FAILURE", to_string(newStatus));
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
  std::string sql = R"SQL(
  WITH per_job(job_id, total_report_retries, report_failure_log,
               archive_request_id, req_job_count) AS (
    VALUES )SQL";
  sql += values_rows;
  sql += R"SQL(
  )
  UPDATE ARCHIVE_ACTIVE_QUEUE a SET
    STATUS = 'ReadyForDeletion'::ARCHIVE_JOB_STATUS,
    TOTAL_REPORT_RETRIES = d.total_report_retries,
    IS_REPORTING = FALSE,
    REPORT_FAILURE_LOG = d.report_failure_log,
    LAST_UPDATE_TIME = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT
  FROM per_job d
  WHERE (d.req_job_count = 1 AND a.JOB_ID = d.job_id)
     OR (d.req_job_count > 1 AND a.ARCHIVE_REQUEST_ID = d.archive_request_id)
  RETURNING a.JOB_ID
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
  // JOB_IDs come straight back from the DB, so they are safe to inline in statement 2. The Rset is
  // scoped: executeQuery() runs in single-row mode and marks the connection async-in-progress, and
  // any further statement on that connection throws until the result is exhausted or released.
  std::string settledJobIds;
  {
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      if (!settledJobIds.empty()) {
        settledJobIds += ",";
      }
      settledJobIds += std::to_string(rset.columnUint64("JOB_ID"));
    }
  }
  if (settledJobIds.empty()) {
    // Nothing matched - the jobs were cancelled or already collected in the meantime.
    txn.setRowCountForTelemetry(0);
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
  txn.setRowCountForTelemetry(nrows);
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

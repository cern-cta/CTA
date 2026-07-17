/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "scheduler/rdbms/postgres/Transaction.hpp"

namespace cta::schedulerdb::postgres {

/**
* @brief Updates or inserts the last fetch timestamp for a mount in the archive job queue.
*
* This function ensures that the MOUNT_QUEUE_LAST_FETCH table has the most recent
* timestamp for a given mount and queue type (MOUNT_ID, QUEUE_TYPE) is a unique identifier in the table.
* It inserts a new row if none exists or updates the LAST_UPDATE_TIME for an existing row,
*  but only if the current stored timestamp is at least 5 seconds older than the new timestamp
*  (write optimisation).
*
* The queue type is determined based on the `isRepack` and `isActive` flags:
* - If `isRepack` is true: "REPACK_ARCHIVE"
* - If `isRepack` is false: "ARCHIVE"
* - In addition "_ACTIVE" is appended to queue type string
* - if `isActive` is true, otherwise "_PENDING" is appended
*
* @param txn      Reference to the active database transaction.
* @param mountId  ID of the mount whose last fetch timestamp is being updated.
* @param isActive Indicates if the active queue or pending queue types are supposed to be updated.
* @param isRepack Indicates if the queue type is for repack or user workflows.
* @param isArchive Indicates if the queue type is for user archival (true) user retrieval (false).
*
* @return The number of rows affected by the insert or update operation.
*/
uint64_t updateMountQueueLastFetch(Transaction& txn, uint64_t mountId, bool isActive, bool isRepack, bool isArchive);

// The status of repack request is being changed by two progress update methods
// one for failures and one for successes (both used for retrieve as well as archive)
// To make sure there is one source of truth, we here are the helper objects for
// building the status related SQL used in these two methods.

/**
 * SQL expression inputs used to build the status and completion expressions for
 * REPACK_REQUEST_TRACKING updates.
 *
 * Each field shall contain a valid SQL expression representing either an
 * existing column or an updated counter expression (e.g.
 * "trk.RETRIEVED_FILES + agg.RETRIEVED_FILES_INC").
 */

struct RepackRequestStatusSqlInputs {
  std::string isExpandFinished;
  std::string retrievedFiles;
  std::string failedRetrieveFiles;
  std::string totalRetrieveFiles;
  std::string archivedFiles;
  std::string failedArchiveFiles;
  std::string failedCreateArchiveReq;
  std::string totalArchiveFiles;
};

/**
 * Builds the SQL boolean expression determining whether a repack request has
 * reached a terminal state.
 *
 * The returned string is a valid PostgreSQL boolean expression that evaluates
 * to TRUE when:
 *   - expansion has finished,
 *   - all files have either been successfully retrieved or failed retrieval,
 *   - all files have either been successfully archived, failed
 *     archiving, or failed during archive request creation.
 *
 * The returned expression is intended to be embedded directly into SQL
 * statements (e.g. CASE expressions or assignments) and does not include any
 * surrounding SQL keywords.
 *
 * @param i SQL expressions representing the current or updated counter values.
 * @return A PostgreSQL boolean expression.
 */
std::string repackRequestIsCompleteSql(const RepackRequestStatusSqlInputs& i);

/**
 * Builds the SQL CASE expression computing the REPACK_REQUEST_TRACKING.STATUS
 * value from the supplied counter expressions.
 *
 * The generated expression implements the repack request status policy:
 *   - Failed    : terminal state with at least one retrieval or archive failure.
 *   - Complete  : terminal state with no retrieval or archive failures.
 *   - Running   : any retrieval/archive progress or failure has been observed.
 *   - Starting  : no progress has been made yet.
 *
 * The returned SQL is intended to be used directly as the right-hand side of a
 * STATUS assignment within an UPDATE statement.
 *
 * @param i SQL expressions representing the current or updated counter values.
 * @return A PostgreSQL CASE expression yielding a REPACK_REQ_STATUS value.
 */
std::string repackRequestStatusSql(const RepackRequestStatusSqlInputs& i);

};  // namespace cta::schedulerdb::postgres

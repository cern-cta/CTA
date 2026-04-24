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

};  // namespace cta::schedulerdb::postgres

/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "rdbms/Conn.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

namespace cta::maintd {
// TO-DO split into Archive/Retrieve/Repack PendingQueueCleanup/ActiveQueueCleanup/FailedQueueCleanup
/**
 * The QueueCleanup should regularly look at the ARCHIVE_PENDING_QUEUE for
 * any jobs with assigned MOUNT_ID for which there are no active MOUNTS and sets their
 * MOUNT_ID to NULL freeing them to be requeued to new drive queues.
 * In addition, it checks the ARCHIVE_ACTIVE_QUEUE table where the drive queues are
 * and checks if there are any jobs for which there was no update since a very long period of time.
 * In that case, it looks up the status of the mount and if active, does nothing,
 * if dead then it cleans the task queue
 * and moved the jobs back to ARCHIVE_PENDING_QUEUE so that they can be queued again.
 */
class MountQueueCleanup : public IRoutine {
  // DatabaseQueueCleanupRoutine
public:
  MountQueueCleanup(log::LogContext &lc, catalogue::Catalogue &catalogue, RelationalDB &pgs, size_t batchSize);

  void execute();

  // Since I don't want to change this file further, I just put it here.
  // We should be more specific in the name of this routine though
  // Generic "queuecleanuproutine" is not great; better to have small focused routines
  std::string getName() const final { return "MountQueueCleanup"; };

private:
  cta::log::LogContext& m_lc;
  cta::RelationalDB& m_RelationalDB;
  catalogue::Catalogue& m_catalogue;
  size_t m_batchSize;
};

}  // namespace cta::maintd

/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
*/

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/Timer.hpp"
#include "scheduler/Scheduler.hpp"

/**
 * Plan => Cleanup keeps track of queues that need to be emptied
 * If a queue is signaled for cleanup, this should take ownership of it, and move all the requests
 * to other queues.
 * If there is no other queue available, the request should be aborted and reported back to the user.
 */

namespace cta::objectstore {

class RetrieveRequest;

class QueueCleanup {
public:
  QueueCleanup(cta::log::LogContext& lc, SchedulerDatabase& oStoreDb, catalogue::Catalogue& catalogue, int batchSize);

  ~QueueCleanup() = default;

  void cleanupQueues();

private:
  cta::log::LogContext& m_lc;
  catalogue::Catalogue& m_catalogue;
  SchedulerDatabase& m_db;
  cta::utils::Timer m_timer;

  int m_batchSize;
};

}  // namespace cta::objectstore

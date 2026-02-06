/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
*/

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/utils/Timer.hpp"
#include "maintd/IRoutine.hpp"
#include "objectstore/QueueCleanup.hpp"
#include "scheduler/Scheduler.hpp"

/**
 * Plan => Cleanup routine keeps track of queues that need to be emptied
 * If a queue is signaled for cleanup, the cleanup routine should take ownership of it, and move all the requests
 * to other queues.
 * If there is no other queue available, the request should be aborted and reported back to the user.
 */

namespace cta::maintd {

class RetrieveRequest;

class QueueCleanupRoutine : public IRoutine {
public:
  QueueCleanupRoutine(cta::log::LogContext& lc,
                      SchedulerDatabase& oStoreDb,
                      catalogue::Catalogue& catalogue,
                      int batchSize);

  ~QueueCleanupRoutine() = default;

  std::string getName() const final;

  void execute() final;

private:
  cta::log::LogContext& m_lc;
  cta::objectstore::QueueCleanup m_queueCleanup;
};

}  // namespace cta::maintd

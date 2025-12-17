/*
* @project      The CERN Tape Archive (CTA)
* @copyright    Copyright © 2021-2022 CERN
* @license      This program is free software, distributed under the terms of the GNU General Public
*               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
*               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
*               option) any later version.
*
*               This program is distributed in the hope that it will be useful, but WITHOUT ANY
*               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
*               PARTICULAR PURPOSE. See the GNU General Public License for more details.
*
*               In applying this licence, CERN does not waive the privileges and immunities
*               granted to it by virtue of its status as an Intergovernmental Organization or
*               submit itself to any jurisdiction.
*/

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/Timer.hpp"
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

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

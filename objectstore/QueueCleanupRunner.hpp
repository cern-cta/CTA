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

#include "objectstore/Agent.hpp"
#include "objectstore/AgentRegister.hpp"
#include "objectstore/AgentWatchdog.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/Sorter.hpp"
#include "catalogue/Catalogue.hpp"

/**
 * Plan => Cleanup runner keeps track of queues that need to be emptied
 * If a queue is signaled for cleanup, the cleanup runner should take ownership of it, and move all the requests
 * to other queues.
 * If there is no other queue available, the request should be aborted and reported back to the user.
 */

namespace cta { namespace objectstore {

class RetrieveRequest;

class QueueCleanupRunner {

public:
  // We currently got for a hardcoded number of jobs batch to requeue every turn
  static constexpr int DEFAULT_BATCH_SIZE = 500;
  static constexpr double DEFAULT_HEARTBEAT_TIMEOUT = 120;

  QueueCleanupRunner(AgentReference &agentReference, SchedulerDatabase & oStoreDb, catalogue::Catalogue &catalogue,
                     std::optional<double> heartBeatTimeout = std::nullopt, std::optional<int> batchSize = std::nullopt);

  ~QueueCleanupRunner();

  void runOnePass(log::LogContext &lc);

private:

  struct QueueCleanupInfo {
    std::string vid;
    cta::common::dataStructures::Tape::State tapeState;
  };

  struct HeartbeatStatus {
    std::string agent;
    uint64_t heartbeat;
    double lastUpdateTimestamp;
  };

  catalogue::Catalogue &m_catalogue;
  SchedulerDatabase &m_db;
  std::map<std::string, HeartbeatStatus> m_heartbeatCheck;
  cta::utils::Timer m_timer;

  int m_batchSize;
  double m_heartBeatTimeout;
};

}
}
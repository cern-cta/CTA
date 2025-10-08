/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

namespace cta::objectstore {

class RetrieveRequest;

class QueueCleanupRunner {

public:
  // We currently got for a hardcoded number of jobs batch to requeue every turn
  static constexpr int DEFAULT_BATCH_SIZE = 500;
  static constexpr double DEFAULT_HEARTBEAT_TIMEOUT = 120;

  QueueCleanupRunner(AgentReference &agentReference, SchedulerDatabase & oStoreDb, catalogue::Catalogue &catalogue,
                     std::optional<double> heartBeatTimeout = std::nullopt, std::optional<int> batchSize = std::nullopt);

  ~QueueCleanupRunner() = default;

  void runOnePass(log::LogContext &lc);

private:

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

} // namespace cta::objectstore

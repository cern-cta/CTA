/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/LogContext.hpp"
#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "AgentWatchdog.hpp"
#include "GenericObject.hpp"
#include "Sorter.hpp"

/**
 * Plan => Garbage collector keeps track of the agents.
 * If an agent is declared dead => take ownership of owned objects
 * Using the backup owner, re-post the object to the container.
 * All containers will have a "repost" method, which is more thorough
 * (and expensive) than the usual one. It can for example prevent double posting.
 */

namespace cta::objectstore {

class GarbageCollector {
public:
  GarbageCollector(cta::log::LogContext& lc,
                   Backend& os,
                   AgentReference& agentReference,
                   cta::catalogue::Catalogue& catalogue);
  ~GarbageCollector();
  void runOnePass();

  void acquireTargets();

  void trimGoneTargets();

  void checkHeartbeats();

  void cleanupDeadAgent(const std::string& name, const std::list<cta::log::Param>& agentDetails);

  /** Structure allowing the sorting of owned objects, so they can be requeued in batches,
    * one batch per queue. */
  struct OwnedObjectSorter {
    //tuple[0] = containerIdentifier (tapepool or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=tapepoolOfTheJob
    std::map<std::tuple<std::string, cta::common::dataStructures::JobQueueType, std::string>,
             std::list<std::shared_ptr<ArchiveRequest>>>
      archiveQueuesAndRequests;
    //tuple[0] = containerIdentifier (vid or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=vidOfTheJob
    std::map<std::tuple<std::string, cta::common::dataStructures::JobQueueType, std::string>,
             std::list<std::shared_ptr<RetrieveRequest>>>
      retrieveQueuesAndRequests;
    std::list<std::shared_ptr<GenericObject>> otherObjects;
    /// Fill up the fetchedObjects with objects of interest.
    void fetchOwnedObjects(Agent& agent,
                           std::list<std::shared_ptr<GenericObject>>& fetchedObjects,
                           Backend& objectStore,
                           cta::log::LogContext& lc) const;
    /// Fill up the sorter with the fetched objects
    void sortFetchedObjects(Agent& agent,
                            std::list<std::shared_ptr<GenericObject>>& fetchedObjects,
                            Backend& objectStore,
                            cta::catalogue::Catalogue& catalogue,
                            cta::log::LogContext& lc);
    /// Lock, fetch and update archive jobs
    void lockFetchAndUpdateArchiveJobs(Agent& agent,
                                       AgentReference& agentReference,
                                       Backend& objectStore,
                                       cta::log::LogContext& lc);
    /// Lock, fetch and update retrieve jobs
    void lockFetchAndUpdateRetrieveJobs(Agent& agent,
                                        AgentReference& agentReference,
                                        Backend& objectStore,
                                        cta::log::LogContext& lc);
    // Lock, fetch and update other objects
    void lockFetchAndUpdateOtherObjects(Agent& agent,
                                        AgentReference& agentReference,
                                        Backend& objectStore,
                                        cta::catalogue::Catalogue& catalogue,
                                        cta::log::LogContext& lc) const;

  private:
    std::string dispatchArchiveAlgorithms(const std::list<std::shared_ptr<ArchiveRequest>>& jobs,
                                          const cta::common::dataStructures::JobQueueType& jobQueueType,
                                          const std::string& containerIdentifier,
                                          const std::string& tapepool,
                                          std::set<std::string, std::less<>>& jobsIndividuallyGCed,
                                          Agent& agent,
                                          AgentReference& agentReference,
                                          Backend& objectstore,
                                          cta::log::LogContext& lc);

    template<typename ArchiveSpecificQueue>
    void executeArchiveAlgorithm(const std::list<std::shared_ptr<ArchiveRequest>>& jobs,
                                 std::string& queueAddress,
                                 const std::string& containerIdentifier,
                                 const std::string& tapepool,
                                 std::set<std::string, std::less<>>& jobsIndividuallyGCed,
                                 Agent& agent,
                                 AgentReference& agentReference,
                                 Backend& objectStore,
                                 cta::log::LogContext& lc);
  };

private:
  cta::log::LogContext& m_lc;
  Backend& m_objectStore;
  cta::catalogue::Catalogue& m_catalogue;
  AgentReference& m_ourAgentReference;
  AgentRegister m_agentRegister;
  std::map<std::string, AgentWatchdog*, std::less<>> m_watchedAgents;
};

}  // namespace cta::objectstore

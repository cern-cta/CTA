/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "AgentWatchdog.hpp"
#include "GenericObject.hpp"
#include "Sorter.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/LogContext.hpp"

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

  void cleanupDeadAgent(const std::string& name, const std::vector<cta::log::Param>& agentDetails);

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

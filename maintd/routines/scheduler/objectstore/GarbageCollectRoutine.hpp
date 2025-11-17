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
#include "maintd/IRoutine.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentRegister.hpp"
#include "objectstore/AgentWatchdog.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/Sorter.hpp"

/**
 * Plan => Garbage collect routine keeps track of the agents.
 * If an agent is declared dead => take ownership of owned objects
 * Using the backup owner, re-post the object to the container.
 * All containers will have a "repost" method, which is more thorough
 * (and expensive) than the usual one. It can for example prevent double posting.
 */

namespace cta::maintd {

class GarbageCollectRoutine : public IRoutine {
public:
  GarbageCollectRoutine(cta::log::LogContext& lc,
                        cta::objectstore::Backend& os,
                        cta::objectstore::AgentReference& agentReference,
                        cta::catalogue::Catalogue& catalogue);
  ~GarbageCollectRoutine() final;
  void execute() final;

  void acquireTargets();

  void trimGoneTargets();

  void checkHeartbeats();

  void cleanupDeadAgent(const std::string& name, const std::list<cta::log::Param>& agentDetails);

  /** Structure allowing the sorting of owned objects, so they can be requeued in batches,
    * one batch per queue. */
  struct OwnedObjectSorter {
    //tuple[0] = containerIdentifier (tapepool or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=tapepoolOfTheJob
    std::map<std::tuple<std::string, cta::common::dataStructures::JobQueueType, std::string>,
             std::list<std::shared_ptr<cta::objectstore::ArchiveRequest>>>
      archiveQueuesAndRequests;
    //tuple[0] = containerIdentifier (vid or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=vidOfTheJob
    std::map<std::tuple<std::string, cta::common::dataStructures::JobQueueType, std::string>,
             std::list<std::shared_ptr<cta::objectstore::RetrieveRequest>>>
      retrieveQueuesAndRequests;
    std::list<std::shared_ptr<cta::objectstore::GenericObject>> otherObjects;
    /// Fill up the fetchedObjects with objects of interest.
    void fetchOwnedObjects(cta::objectstore::Agent& agent,
                           std::list<std::shared_ptr<cta::objectstore::GenericObject>>& fetchedObjects,
                           cta::objectstore::Backend& objectStore,
                           cta::log::LogContext& lc) const;
    /// Fill up the sorter with the fetched objects
    void sortFetchedObjects(cta::objectstore::Agent& agent,
                            std::list<std::shared_ptr<cta::objectstore::GenericObject>>& fetchedObjects,
                            cta::objectstore::Backend& objectStore,
                            cta::catalogue::Catalogue& catalogue,
                            cta::log::LogContext& lc);
    /// Lock, fetch and update archive jobs
    void lockFetchAndUpdateArchiveJobs(cta::objectstore::Agent& agent,
                                       cta::objectstore::AgentReference& agentReference,
                                       cta::objectstore::Backend& objectStore,
                                       cta::log::LogContext& lc);
    /// Lock, fetch and update retrieve jobs
    void lockFetchAndUpdateRetrieveJobs(cta::objectstore::Agent& agent,
                                        cta::objectstore::AgentReference& agentReference,
                                        cta::objectstore::Backend& objectStore,
                                        cta::log::LogContext& lc);
    // Lock, fetch and update other objects
    void lockFetchAndUpdateOtherObjects(cta::objectstore::Agent& agent,
                                        cta::objectstore::AgentReference& agentReference,
                                        cta::objectstore::Backend& objectStore,
                                        cta::catalogue::Catalogue& catalogue,
                                        cta::log::LogContext& lc) const;

  private:
    std::string dispatchArchiveAlgorithms(const std::list<std::shared_ptr<cta::objectstore::ArchiveRequest>>& jobs,
                                          const cta::common::dataStructures::JobQueueType& jobQueueType,
                                          const std::string& containerIdentifier,
                                          const std::string& tapepool,
                                          std::set<std::string, std::less<>>& jobsIndividuallyGCed,
                                          cta::objectstore::Agent& agent,
                                          cta::objectstore::AgentReference& agentReference,
                                          cta::objectstore::Backend& objectstore,
                                          cta::log::LogContext& lc);

    template<typename ArchiveSpecificQueue>
    void executeArchiveAlgorithm(const std::list<std::shared_ptr<cta::objectstore::ArchiveRequest>>& jobs,
                                 std::string& queueAddress,
                                 const std::string& containerIdentifier,
                                 const std::string& tapepool,
                                 std::set<std::string, std::less<>>& jobsIndividuallyGCed,
                                 cta::objectstore::Agent& agent,
                                 cta::objectstore::AgentReference& agentReference,
                                 cta::objectstore::Backend& objectStore,
                                 cta::log::LogContext& lc);
  };

  std::string getName() const override final;

private:
  cta::log::LogContext& m_lc;
  cta::objectstore::Backend& m_objectStore;
  cta::catalogue::Catalogue& m_catalogue;
  cta::objectstore::AgentReference& m_ourAgentReference;
  cta::objectstore::AgentRegister m_agentRegister;
  std::map<std::string, cta::objectstore::AgentWatchdog*, std::less<>> m_watchedAgents;
};

}  // namespace cta::maintd

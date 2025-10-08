  /*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "Agent.hpp"
#include "AgentRegister.hpp"
#include "AgentWatchdog.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/LogContext.hpp"
#include "GenericObject.hpp"
#include "Sorter.hpp"

/**
 * Plan => Garbage collector keeps track of the agents.
 * If an agent is declared dead => take ownership of owned objects
 * Using the backup owner, re-post the objet to the container.
 * All containers will have a "repost" method, which is more thorough
 * (and expensive) than the usual one. It can for example prevent double posting.
 */

namespace cta::objectstore {

class ArchiveRequest;
class RetrieveRequest;

class GarbageCollector {
public:
  GarbageCollector(Backend & os, AgentReference & agentReference, catalogue::Catalogue & catalogue);
  ~GarbageCollector();
  void runOnePass(log::LogContext & lc);

  void acquireTargets(log::LogContext & lc);

  void trimGoneTargets(log::LogContext & lc);

  void checkHeartbeats(log::LogContext & lc);

  void cleanupDeadAgent(const std::string & name, const std::list<log::Param>& agentDetails, log::LogContext & lc);

  /** Structure allowing the sorting of owned objects, so they can be requeued in batches,
    * one batch per queue. */
  struct OwnedObjectSorter {
    //tuple[0] = containerIdentifier (tapepool or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=tapepoolOfTheJob
    std::map<std::tuple<std::string, common::dataStructures::JobQueueType , std::string>, std::list<std::shared_ptr <ArchiveRequest>>> archiveQueuesAndRequests;
    //tuple[0] = containerIdentifier (vid or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=vidOfTheJob
    std::map<std::tuple<std::string, common::dataStructures::JobQueueType, std::string>, std::list<std::shared_ptr <RetrieveRequest>>> retrieveQueuesAndRequests;
    std::list<std::shared_ptr<GenericObject>> otherObjects;
    //Sorter m_sorter;
    /// Fill up the fetchedObjects with objects of interest.
    void fetchOwnedObjects(Agent & agent, std::list<std::shared_ptr<GenericObject>> & fetchedObjects, Backend & objectStore,
        log::LogContext & lc);
    /// Fill up the sorter with the fetched objects
    void sortFetchedObjects(Agent & agent, std::list<std::shared_ptr<GenericObject>> & fetchedObjects, Backend & objectStore,
        cta::catalogue::Catalogue & catalogue, log::LogContext & lc);
    /// Lock, fetch and update archive jobs
    void lockFetchAndUpdateArchiveJobs(Agent & agent, AgentReference & agentReference, Backend & objectStore, log::LogContext & lc);
    /// Lock, fetch and update retrieve jobs
    void lockFetchAndUpdateRetrieveJobs(Agent & agent, AgentReference & agentReference, Backend & objectStore, log::LogContext & lc);
    // Lock, fetch and update other objects
    void lockFetchAndUpdateOtherObjects(Agent & agent, AgentReference & agentReference, Backend & objectStore,
        cta::catalogue::Catalogue & catalogue, log::LogContext & lc);
    //Sorter& getSorter();

  private:
    std::string dispatchArchiveAlgorithms(std::list<std::shared_ptr<ArchiveRequest>> &jobs,const common::dataStructures::JobQueueType& jobQueueType, const std::string& containerIdentifier,
        const std::string& tapepool,std::set<std::string> & jobsIndividuallyGCed,
        Agent& agent, AgentReference& agentReference, Backend & objectstore, log::LogContext &lc);

    template<typename ArchiveSpecificQueue>
    void executeArchiveAlgorithm(std::list<std::shared_ptr<ArchiveRequest>> &jobs,std::string &queueAddress, const std::string& containerIdentifier, const std::string& tapepool,
        std::set<std::string> & jobsIndividuallyGCed, Agent& agent, AgentReference& agentReference,
        Backend &objectStore, log::LogContext& lc);
  };

private:
  Backend & m_objectStore;
  catalogue::Catalogue & m_catalogue;
  AgentReference & m_ourAgentReference;
  AgentRegister m_agentRegister;
  std::map<std::string, AgentWatchdog * > m_watchedAgents;
};

} // namespace cta::objectstore

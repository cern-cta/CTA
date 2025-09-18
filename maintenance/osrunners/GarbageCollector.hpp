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

#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/LogContext.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentRegister.hpp"
#include "objectstore/AgentWatchdog.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/Sorter.hpp"



/**
 * Plan => Garbage collector keeps track of the agents.
 * If an agent is declared dead => take ownership of owned objects
 * Using the backup owner, re-post the objet to the container.
 * All containers will have a "repost" method, which is more thorough
 * (and expensive) than the usual one. It can for example prevent double posting.
 */

namespace cta::maintenance {

//class objectstore::ArchiveRequest;
//class objectstore::RetrieveRequest;

class GarbageCollector {
public:
  GarbageCollector(objectstore::Backend & os, objectstore::AgentReference & agentReference, catalogue::Catalogue & catalogue);
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
    std::map<std::tuple<std::string, common::dataStructures::JobQueueType , std::string>, std::list<std::shared_ptr <objectstore::ArchiveRequest>>> archiveQueuesAndRequests;
    //tuple[0] = containerIdentifier (vid or Repack Request's address), tuple[1]=jobQueueType, tuple[2]=vidOfTheJob
    std::map<std::tuple<std::string, common::dataStructures::JobQueueType, std::string>, std::list<std::shared_ptr <objectstore::RetrieveRequest>>> retrieveQueuesAndRequests;
    std::list<std::shared_ptr<objectstore::GenericObject>> otherObjects;
    /// Fill up the fetchedObjects with objects of interest.
    void fetchOwnedObjects(objectstore::Agent & agent, std::list<std::shared_ptr<objectstore::GenericObject>> & fetchedObjects, objectstore::Backend & objectStore,
        log::LogContext & lc);
    /// Fill up the sorter with the fetched objects
    void sortFetchedObjects(objectstore::Agent & agent, std::list<std::shared_ptr<objectstore::GenericObject>> & fetchedObjects, objectstore::Backend & objectStore,
        cta::catalogue::Catalogue & catalogue, log::LogContext & lc);
    /// Lock, fetch and update archive jobs
    void lockFetchAndUpdateArchiveJobs(objectstore::Agent & agent, objectstore::AgentReference & agentReference, objectstore::Backend & objectStore, log::LogContext & lc);
    /// Lock, fetch and update retrieve jobs
    void lockFetchAndUpdateRetrieveJobs(objectstore::Agent & agent, objectstore::AgentReference & agentReference, objectstore::Backend & objectStore, log::LogContext & lc);
    // Lock, fetch and update other objects
    void lockFetchAndUpdateOtherObjects(objectstore::Agent & agent, objectstore::AgentReference & agentReference, objectstore::Backend & objectStore,
        cta::catalogue::Catalogue & catalogue, log::LogContext & lc);

  private:
    std::string dispatchArchiveAlgorithms(std::list<std::shared_ptr<objectstore::ArchiveRequest>> &jobs,const common::dataStructures::JobQueueType& jobQueueType, const std::string& containerIdentifier,
        const std::string& tapepool,std::set<std::string> & jobsIndividuallyGCed,
        objectstore::Agent& agent, objectstore::AgentReference& agentReference, objectstore::Backend & objectstore, log::LogContext &lc);

    template<typename ArchiveSpecificQueue>
    void executeArchiveAlgorithm(std::list<std::shared_ptr<objectstore::ArchiveRequest>> &jobs,std::string &queueAddress, const std::string& containerIdentifier, const std::string& tapepool,
        std::set<std::string> & jobsIndividuallyGCed, objectstore::Agent& agent, objectstore::AgentReference& agentReference,
        objectstore::Backend &objectStore, log::LogContext& lc);
  };

private:
  objectstore::Backend & m_objectStore;
  catalogue::Catalogue & m_catalogue;
  objectstore::AgentReference & m_ourAgentReference;
  objectstore::AgentRegister m_agentRegister;
  std::map<std::string, objectstore::AgentWatchdog * > m_watchedAgents;
};

} // namespace cta::maintenance

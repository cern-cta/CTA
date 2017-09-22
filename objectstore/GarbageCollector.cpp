/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "GarbageCollector.hpp"
#include "AgentReference.hpp"
#include "RootEntry.hpp"
#include "Helpers.hpp"
#include <algorithm>

namespace cta { namespace objectstore {
const size_t GarbageCollector::c_maxWatchedAgentsPerGC = 25;

GarbageCollector::GarbageCollector(Backend & os, AgentReference & agentReference, catalogue::Catalogue & catalogue): 
  m_objectStore(os), m_catalogue(catalogue), m_ourAgentReference(agentReference), m_agentRegister(os) {
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  m_agentRegister.setAddress(re.getAgentRegisterAddress());
  reLock.release();
  ScopedSharedLock arLock(m_agentRegister);
  m_agentRegister.fetch();
}

void GarbageCollector::runOnePass(log::LogContext & lc) {
  trimGoneTargets(lc);
  aquireTargets(lc);
  checkHeartbeats(lc);
}
  
void GarbageCollector::trimGoneTargets(log::LogContext & lc) {
  ScopedSharedLock arLock(m_agentRegister);
  m_agentRegister.fetch();
  arLock.release();
  std::list<std::string> agentList = m_agentRegister.getAgents();
  for (std::map<std::string, AgentWatchdog * >::iterator wa
        = m_watchedAgents.begin();
      wa != m_watchedAgents.end();) {
    if (agentList.end() == std::find(agentList.begin(), agentList.end(), wa->first)) {
      Agent ourAgent(m_ourAgentReference.getAgentAddress(), m_objectStore);
      ScopedExclusiveLock oaLock(ourAgent);
      ourAgent.fetch();
      ourAgent.removeFromOwnership(wa->first);
      std::string removedAgent = wa->first;
      ourAgent.commit();
      oaLock.release();
      delete wa->second;
      m_watchedAgents.erase(wa++);
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", removedAgent);
      lc.log(log::INFO, "In GarbageCollector::trimGoneTargets(): removed now gone agent.");
    } else {
      wa++;
    }
  }
}
  
void GarbageCollector::aquireTargets(log::LogContext & lc) {
  ScopedExclusiveLock arLock(m_agentRegister);
  m_agentRegister.fetch();
  // Get the list of untracked agents
  std::list<std::string> candidatesList = m_agentRegister.getUntrackedAgents();
  std::list<std::string>::const_iterator c = candidatesList.begin();
  // We can now take ownership of new agents, up to our max...
  // and we don't monitor ourselves!
  for (;m_watchedAgents.size() < c_maxWatchedAgentsPerGC
         && c!=candidatesList.end(); c++) {
    // We don't monitor ourselves
    if (*c != m_ourAgentReference.getAgentAddress()) {
      // So we have a candidate we might want to monitor
      // First, check that the agent entry exists, and that ownership
      // is indeed pointing to the agent register
      Agent ag(*c, m_objectStore);
      ScopedExclusiveLock agLock;
      Agent ourAgent(m_ourAgentReference.getAgentAddress(), m_objectStore);
      ScopedExclusiveLock oaLock;
      try {
        if (!ag.exists()) {
          // This is a dangling pointer to a dead object:
          // remove it in the agentRegister.
          m_agentRegister.removeAgent(*c);
          continue;
        }
        agLock.lock(ag);
        ag.fetch();
        // Check that the actual owner is the agent register.
        // otherwise, it should not be listed as an agent to monitor
        if (ag.getOwner() != m_agentRegister.getAddressIfSet()) {
          m_agentRegister.trackAgent(ag.getAddressIfSet());
          agLock.release();
          continue;
        }
        // We are now interested in tracking this agent. So we will transfer its
        // ownership. We alredy have an exclusive lock on the agent.
        // Lock ours
        
        oaLock.lock(ourAgent);
        ourAgent.fetch();
        ourAgent.addToOwnership(ag.getAddressIfSet());
        ourAgent.commit();
        // We now have a pointer to the agent, we can make the ownership official
        ag.setOwner(ourAgent.getAddressIfSet());
        ag.commit();
      } catch (cta::exception::Exception & ex) {
        // We received an exception. This can happen is the agent disappears under our feet.
        // This is fine, we just let go this time, and trimGoneTargets() will just de-reference
        // it later. But if the object is present, we have a problem.
        if (m_objectStore.exists(*c)) throw;
      }
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", ag.getAddressIfSet())
            .add("gcAgentAddress", ourAgent.getAddressIfSet());
      lc.log(log::INFO, "In GarbageCollector::aquireTargets(): started tracking an untracked agent");
      // Agent is officially ours, we can remove it from the untracked agent's
      // list
      m_agentRegister.trackAgent(ag.getAddressIfSet());
      m_agentRegister.commit();
      // Agent is now officially ours, let's track it. We have the release the 
      // lock to the agent before constructing the watchdog, which builds
      // its own agent objects (and need to lock the object store representation)
      std::string agentName = ag.getAddressIfSet();
      double timeout=ag.getTimeout();
      agLock.release();      
      m_watchedAgents[agentName] =
        new AgentWatchdog(agentName, m_objectStore);
      m_watchedAgents[ag.getAddressIfSet()]->setTimeout(timeout);
    }
  }
  // Commit all the modifications to the agent register
  m_agentRegister.commit();
}
 
void GarbageCollector::checkHeartbeats(log::LogContext & lc) {
  // Check the heartbeats of the watched agents
  // We can still fail on many steps
  for (std::map<std::string, AgentWatchdog * >::iterator wa = m_watchedAgents.begin();
      wa != m_watchedAgents.end();) {
    // Get the heartbeat. Clean dead agents and remove references to them
    try {
      if (!wa->second->checkAlive()) {
        cleanupDeadAgent(wa->first, lc);
        Agent ourAgent(m_ourAgentReference.getAgentAddress(), m_objectStore);
        ScopedExclusiveLock oaLock(ourAgent);
        ourAgent.fetch();
        ourAgent.removeFromOwnership(wa->first);
        ourAgent.commit();
        delete wa->second;
        m_watchedAgents.erase(wa++);
      } else {
        wa++;
      }
    } catch (cta::exception::Exception & ex) {
      if (wa->second->checkExists()) {
        // We really have a problem: we failed to check on an agent, that is still present.
        throw;
      } else {
        // The agent is simply gone on the wrong time. It will be trimmed from the list on the next pass.
        wa++;
      }
    }
  }
}

void GarbageCollector::cleanupDeadAgent(const std::string & address, log::LogContext & lc) {
  // Check that we are still owners of the agent (sanity check).
  Agent agent(address, m_objectStore);
  ScopedExclusiveLock agLock(agent);
  agent.fetch();
  log::ScopedParamContainer params(lc);
  params.add("agentAddress", agent.getAddressIfSet())
        .add("gcAgentAddress", m_ourAgentReference.getAgentAddress());
  if (agent.getOwner() != m_ourAgentReference.getAgentAddress()) {
   log::ScopedParamContainer params(lc);
   lc.log(log::WARNING, "In GarbageCollector::cleanupDeadAgent(): skipping agent which is not owned by this garbage collector as thought.");
   // The agent is removed from our ownership by the calling function: we're done.
   return;
  }
  lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): will cleanup dead agent.");
  // Return all objects owned by the agent to their respective backup owners
    
  const auto ownedObjectAddresses = agent.getOwnershipList();
  // Parallel fetch (lock free) all the objects to assess their status (check ownership,
  // type and decide to which queue they will go.
  std::list<std::shared_ptr<GenericObject>> ownedObjects;
  std::map<GenericObject *, std::unique_ptr<GenericObject::AsyncLockfreeFetcher>> ownedObjectsFetchers;
  for (auto & obj : ownedObjectAddresses) {
    // Create the generic objects and fetch them
    ownedObjects.emplace_back(new GenericObject(obj, m_objectStore));
    if (ownedObjects.back()->exists()) {
      ownedObjectsFetchers[ownedObjects.back().get()].reset(ownedObjects.back()->asyncLockfreeFetch());
    } else {
      agent.removeFromOwnership(ownedObjects.back()->getAddressIfSet());
      agent.commit();
      ownedObjects.pop_back();
      lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): skipping garbage collection of now gone object.");
    }
  }
  
  // We will now sort the objects this agent really owns
  OwnedObjectSorter ownedObjectSorter;
  using serializers::ArchiveJobStatus;
  std::set<ArchiveJobStatus> inactiveArchiveJobStatuses({ArchiveJobStatus::AJS_Complete, ArchiveJobStatus::AJS_Failed});
  using serializers::RetrieveJobStatus;
  std::set<RetrieveJobStatus> inactiveRetrieveJobStatuses({RetrieveJobStatus::RJS_Complete, RetrieveJobStatus::RJS_Failed});
  for (auto & obj : ownedObjects) {
    log::ScopedParamContainer params2(lc);
    params2.add("objectAddress", obj->getAddressIfSet());
    ownedObjectsFetchers.at(obj.get())->wait();
    ownedObjectsFetchers.erase(obj.get());
    if (obj->getOwner() != agent.getAddressIfSet()) {
      // For all object types except ArchiveRequests, this means we do
      // no need to deal with this object.
      if (obj->type() == serializers::ArchiveRequest_t) {
        ArchiveRequest ar(*obj);
        for (auto & j:ar.dumpJobs()) if (j.owner == agent.getAddressIfSet()) goto doGCObject;
      }
      lc.log(log::WARNING, "In GarbageCollector::cleanupDeadAgent(): skipping object which is not owned by this agent");
      continue;
    }
  doGCObject:
    switch (obj->type()) {
      case serializers::ArchiveRequest_t:
      {
        // We need to find out in which queue or queues the owned job(s)
        // Decision is simple: if the job is owned and active, it needs to be requeued
        // in its destination archive queue.
        // Get hold of an (unlocked) archive request:
        std::shared_ptr<ArchiveRequest> ar(new ArchiveRequest(*obj));
        bool jobRequeued=false;
        for (auto &j: ar->dumpJobs()) {
          if ((j.owner == agent.getAddressIfSet() || ar->getOwner() == agent.getAddressIfSet())
              && !inactiveArchiveJobStatuses.count(j.status)) {
            ownedObjectSorter.archiveQueuesAndRequests[j.tapePool].emplace_back(ar);
            log::ScopedParamContainer params3(lc);
            params3.add("tapepool", j.tapePool)
                   .add("copynb", j.copyNb)
                   .add("fileId", ar->getArchiveFile().archiveFileID);
            lc.log(log::INFO, "Selected archive request for requeueing to tape pool");
          }
        }
        if (!jobRequeued) {
          log::ScopedParamContainer params3(lc);
          params3.add("fileId", ar->getArchiveFile().archiveFileID);
          lc.log(log::INFO, "No active archive job to requeue found. Request will remain as-is.");
        }
        break;
      }
      case serializers::RetrieveRequest_t:
      {
        // We need here to re-determine the best tape (and queue) for the retrieve request.
        std::shared_ptr<RetrieveRequest> rr(new RetrieveRequest(*obj));
        // Get the list of vids for non failed tape files.
        std::set<std::string> candidateVids;
        for (auto & j: rr->dumpJobs()) {
          if (!inactiveRetrieveJobStatuses.count(j.status)) {
            candidateVids.insert(rr->getArchiveFile().tapeFiles.at(j.copyNb).vid);
          }
        }
        if (candidateVids.empty()) {
          log::ScopedParamContainer params3(lc);
          params3.add("fileId", rr->getArchiveFile().archiveFileID);
          lc.log(log::INFO, "No active retrieve job to requeue found. Marking request for deletion.");
          ownedObjectSorter.retrieveRequestsToDelete.emplace_back(rr);
        } else {
          auto vid=Helpers::selectBestRetrieveQueue(candidateVids, m_catalogue, m_objectStore);
          ownedObjectSorter.retrieveQueuesAndRequests[vid].emplace_back(rr);
          log::ScopedParamContainer params3(lc);
          // Find copyNb for logging
          size_t copyNb = std::numeric_limits<size_t>::max();
          uint64_t fSeq = std::numeric_limits<uint64_t>::max();
          for (auto & tc: rr->getArchiveFile().tapeFiles) { if (tc.second.vid==vid) { copyNb=tc.first; fSeq=tc.second.fSeq; } }
          params3.add("fileId", rr->getArchiveFile().archiveFileID)
                 .add("copyNb", copyNb)
                 .add("vid", vid)
                 .add("fSeq", fSeq);
          lc.log(log::INFO, "Selected vid to be requeued for retrieve request.");
        }
        break;
      }
      case serializers::Agent_t:
      {
        // Check agent ownership and add to list.
        std::shared_ptr<Agent> ag(new Agent (*obj));
        ownedObjectSorter.agents.emplace_back(ag);
        lc.log(log::INFO, "Will mark agent as not owned.");
        break;
      }
      default:
        ownedObjectSorter.otherObjects.emplace_back(obj);
        break;
    }
    // We can now get rid of the generic object (data was transferred in a (typed) object in the sorter).
    // Not yet as we still run the old garbage collection.
    //obj.reset();
  }
  
  // We can now start updating the objects efficiently.
  
  
  /**
   * old code to compile and unit tests
   */
  for (auto & obj : ownedObjects) { 
   // Find the object
   GenericObject go(obj->getAddressIfSet(), m_objectStore);
   log::ScopedParamContainer params2(lc);
   params2.add("objectAddress", go.getAddressIfSet());
   // If the object does not exist, we're done.
   if (go.exists()) {
     ScopedExclusiveLock goLock(go);
     go.fetch();
     // Call GenericOpbject's garbage collect method, which in turn will
     // delegate to the object type's garbage collector.
     go.garbageCollectDispatcher(goLock, address, m_ourAgentReference, lc, m_catalogue);
     lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): garbage collected owned object.");
   } else {
     lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): skipping garbage collection of now gone object.");
   }
   // In all cases, relinquish ownership for this object
   agent.removeFromOwnership(obj->getAddressIfSet());
   agent.commit();
  }
  // We now processed all the owned objects. We can delete the agent's entry
  agent.removeAndUnregisterSelf();
  lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): agent entry removed.");
}

void GarbageCollector::reinjectOwnedObject(log::LogContext& lc) {
  // We have to release the agents we were following. PErformance is not an issue, so
  // we go in small steps.
  // First check the agents are indeed owned by us and still exist.
  std::list<std::string> stillTrackedAgents;
  std::list<std::string> goneAgents;
  std::list<std::string> notReallyOwnedAgents;
  std::list<std::string> inaccessibleAgents;
  {
    auto a = m_watchedAgents.begin();
    while(a!=m_watchedAgents.end()) {
      auto & agentAddress=a->first;
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", agentAddress);
      // Check the agent is there, and ours.
      if (!m_objectStore.exists(agentAddress)) {
        goneAgents.emplace_back(agentAddress);
        lc.log(log::INFO, "In GarbageCollector::reinjectOwnedObject(): agent not present anymore.");
      } else {
        try {
          Agent ag(agentAddress, m_objectStore);
          ScopedSharedLock agl(ag);
          ag.fetch();
          if (ag.getOwner() == m_ourAgentReference.getAgentAddress()) {
            stillTrackedAgents.emplace_back(agentAddress);
            lc.log(log::INFO, "In GarbageCollector::reinjectOwnedObject(): agent still owned by us.");
          } else {
            params.add("currentOwner", ag.getOwner());
            notReallyOwnedAgents.emplace_back(agentAddress);
            lc.log(log::ERR, "In GarbageCollector::reinjectOwnedObject(): agent not owned by us.");
          }
        } catch (cta::exception::Exception & ex) {
          params.add("ExceptionMessage", ex.getMessageValue());
          lc.log(log::ERR, "In GarbageCollector::reinjectOwnedObject(): agent inaccessible.");
          inaccessibleAgents.emplace_back(a->first);
        }
      }
      a=m_watchedAgents.erase(a);
    }
  }
  {
    // We now have an overview of the situation. We can update the agent register based on that.
    ScopedExclusiveLock arLock(m_agentRegister);
    m_agentRegister.fetch();
    for (auto &sta: stillTrackedAgents) {
      m_agentRegister.untrackAgent(sta);
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", sta);
      lc.log(log::INFO, "In GarbageCollector::reinjectOwnedObject(): untracked agent in registry.");
    }
    for (auto &ga: goneAgents) {
      m_agentRegister.removeAgent(ga);
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", ga);
      lc.log(log::INFO, "In GarbageCollector::reinjectOwnedObject(): removed gone agent from registry.");
    }
    // This is all we are going to do. Other agents cannot be acted upon.
    m_agentRegister.commit();
  }
  // We can now remove ownership from the agents we still owned
  for (auto & sta: stillTrackedAgents) {
    log::ScopedParamContainer params(lc);
    params.add("agentAddress", sta);
    Agent ag (sta, m_objectStore);
    ScopedExclusiveLock agl(ag);
    ag.fetch();
    if (ag.getOwner() == m_ourAgentReference.getAgentAddress()) {
      ag.setOwner(m_agentRegister.getAddressIfSet());
      ag.commit();
      lc.log(log::INFO, "In GarbageCollector::reinjectOwnedObject(): changed agent ownership to registry.");
    } else {
      params.add("newOwner", ag.getOwner());
      lc.log(log::ERR, "In GarbageCollector::reinjectOwnedObject(): skipping agent whose ownership we lost last minute.");
    }
  }
  // We can now cleanup our own agent and remove it.
  Agent ourAg(m_ourAgentReference.getAgentAddress(), m_objectStore);
  ScopedExclusiveLock ourAgL(ourAg);
  ourAg.fetch();
  std::list<std::string> allAgents;
  allAgents.splice(allAgents.end(), stillTrackedAgents);
  allAgents.splice(allAgents.end(), notReallyOwnedAgents);
  allAgents.splice(allAgents.end(), inaccessibleAgents);
  allAgents.splice(allAgents.end(), goneAgents);
  for (auto & a: allAgents) {
    log::ScopedParamContainer params(lc);
    params.add("agentAddress", a);
    ourAg.removeFromOwnership(a);
    lc.log(log::ERR, "In GarbageCollector::reinjectOwnedObject(): removed agent from our ownership.");
  }
  ourAg.commit();
}




}}

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
#include <algorithm>

namespace cta { namespace objectstore {

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
  m_agentRegister.fetchNoLock();
  std::list<std::string> agentList = m_agentRegister.getAgents();
  // Find the agents we knew about and are not listed anymore.
  // We will just stop looking for them.
  for (std::map<std::string, AgentWatchdog * >::iterator wa
        = m_watchedAgents.begin();
      wa != m_watchedAgents.end();) {
    if (agentList.end() == std::find(agentList.begin(), agentList.end(), wa->first)) {
      delete wa->second;
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", wa->first);
      m_watchedAgents.erase(wa++);
      lc.log(log::INFO, "In GarbageCollector::trimGoneTargets(): removed now gone agent.");
    } else {
      wa++;
    }
  }
}

void GarbageCollector::aquireTargets(log::LogContext & lc) {
  m_agentRegister.fetchNoLock();
  // We will now watch all agents we do not know about yet.
  std::list<std::string> candidatesList = m_agentRegister.getUntrackedAgents();
  // Build a set of our own tracked agents.
  std::set<std::string> alreadyTrackedAgents;
  for (auto &ata: m_watchedAgents) {
    alreadyTrackedAgents.insert(ata.first);
  }
  for (auto &c: candidatesList) {
    // We don't monitor ourselves
    if (c != m_ourAgentReference.getAgentAddress() && !alreadyTrackedAgents.count(c)) {
      // So we have a candidate we might want to monitor
      // First, check that the agent entry exists, and that ownership
      // is indeed pointing to the agent register
      Agent ag(c, m_objectStore);
      try {
        ag.fetchNoLock();
      } catch (...) {
        // The agent could simply be gone... (If not, let the complain go through).
        if (m_objectStore.exists(c)) throw;
        continue;
      }
      if (ag.getOwner() == m_agentRegister.getAddressIfSet()) {
      }
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", ag.getAddressIfSet())
            .add("gcAgentAddress", m_ourAgentReference.getAgentAddress());
      lc.log(log::INFO, "In GarbageCollector::aquireTargets(): started tracking an untracked agent");
      // Agent is to be tracked, let's track it.
      double timeout=ag.getTimeout();
      m_watchedAgents[c] =
        new AgentWatchdog(c, m_objectStore);
      m_watchedAgents[c]->setTimeout(timeout);
    }
  }
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
  // We detected a dead agent. Try and take ownership of it. It could already be owned
  // by another garbage collector.
  // To minimize locking, take a lock on the agent and check its ownership first.
  // We do not need to be defensive about exception here as calling function will
  // deal with them.
  Agent agent(address, m_objectStore);
  ScopedExclusiveLock agLock(agent);
  agent.fetch();
  log::ScopedParamContainer params(lc);
  params.add("agentAddress", agent.getAddressIfSet())
        .add("gcAgentAddress", m_ourAgentReference.getAgentAddress());
  if (agent.getOwner() != m_agentRegister.getAddressIfSet()) {
    params.add("agentOwner", agent.getOwner());
    lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): skipping agent which is not owned by agent register anymore.");
    // The agent will be removed from our ownership by the calling function: we're done.
    return;
  }
  // Aquire ownership of the agent.
  m_ourAgentReference.addToOwnership(address,m_objectStore);
  agent.setOwner(m_ourAgentReference.getAgentAddress());
  agent.commit();
  // Update the register
  ScopedExclusiveLock arl(m_agentRegister);
  m_agentRegister.fetch();
  m_agentRegister.trackAgent(address);
  m_agentRegister.commit();
  arl.release();
  lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): will cleanup dead agent.");
  // Return all objects owned by the agent to their respective backup owners
  auto ownedObjects = agent.getOwnershipList();
  for (auto obj = ownedObjects.begin(); obj!= ownedObjects.end(); obj++) {
   // Find the object
   GenericObject go(*obj, m_objectStore);
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
   agent.removeFromOwnership(*obj);
   agent.commit();
  }
  // We now processed all the owned objects. We can delete the agent's entry
  agent.removeAndUnregisterSelf();
  lc.log(log::INFO, "In GarbageCollector::cleanupDeadAgent(): agent entry removed.");
  // We can remove the agent from our own ownership.
  m_ourAgentReference.removeFromOwnership(address, m_objectStore);
}

}}

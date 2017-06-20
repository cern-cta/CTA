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
const size_t GarbageCollector::c_maxWatchedAgentsPerGC = 5;

GarbageCollector::GarbageCollector(Backend & os, AgentReference & agentReference): 
  m_objectStore(os), m_ourAgentReference(agentReference), m_agentRegister(os) {
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
      if (!ag.exists()) {
        // This is a dangling pointer to a dead object:
        // remove it in the agentRegister.
        m_agentRegister.removeAgent(*c);
        continue;
      }
      ScopedExclusiveLock agLock(ag);
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
      Agent ourAgent(m_ourAgentReference.getAgentAddress(), m_objectStore);
      ScopedExclusiveLock oaLock(ourAgent);
      ourAgent.fetch();
      ourAgent.addToOwnership(ag.getAddressIfSet());
      ourAgent.commit();
      // We now have a pointer to the agent, we can make the ownership official
      ag.setOwner(ourAgent.getAddressIfSet());
      ag.commit();
      log::ScopedParamContainer params(lc);
      params.add("agentAddress", ag.getAddressIfSet())
            .add("gcAgentAddress", ourAgent.getAddressIfSet());
      lc.log(log::INFO, "In GarbageCollector::aquireTargets(): started tracking an untracked agent");
      // Agent is officially our, we can remove it from the untracked agent's
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
       go.garbageCollectDispatcher(goLock, address, m_ourAgentReference);
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
 }



}}

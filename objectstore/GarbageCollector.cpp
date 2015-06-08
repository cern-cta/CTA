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
#include "RootEntry.hpp"
//#include "FIFO.hpp"
#include <algorithm>

namespace cta { namespace objectstore {
const size_t GarbageCollector::c_maxWatchedAgentsPerGC = 2;

GarbageCollector::GarbageCollector(Backend & os, Agent & agent): 
  m_objectStore(os), m_ourAgent(agent), m_agentRegister(os) {
  RootEntry re(m_objectStore);
  ScopedSharedLock reLock(re);
  re.fetch();
  m_agentRegister.setAddress(re.getAgentRegisterAddress());
  reLock.release();
  ScopedSharedLock arLock(m_agentRegister);
  m_agentRegister.fetch();
}

void GarbageCollector::runOnePass() {
  // Bump our own heart beat
  {
    ScopedExclusiveLock lock (m_ourAgent);
    m_ourAgent.fetch();
    m_ourAgent.bumpHeartbeat();
    m_ourAgent.commit();
  }
  trimGoneTargets();
  aquireTargets();
  checkHeartbeats();
}
  
void GarbageCollector::trimGoneTargets() {
  ScopedSharedLock arLock(m_agentRegister);
  m_agentRegister.fetch();
  arLock.release();
  std::list<std::string> agentList = m_agentRegister.getAgents();
  for (std::map<std::string, AgentWatchdog * >::iterator wa
        = m_watchedAgents.begin();
      wa != m_watchedAgents.end();) {
    if (agentList.end() == std::find(agentList.begin(), agentList.end(), wa->first)) {
      ScopedExclusiveLock oaLock(m_ourAgent);
      m_ourAgent.fetch();
      m_ourAgent.removeFromOwnership(wa->first);
      m_ourAgent.commit();
      oaLock.release();
      delete wa->second;
      m_watchedAgents.erase(wa++);
    } else {
      wa++;
    }
  }
}
  
void GarbageCollector::aquireTargets() {
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
    if (*c != m_ourAgent.getAddressIfSet()) {
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
      ScopedExclusiveLock oaLock(m_ourAgent);
      m_ourAgent.fetch();
      m_ourAgent.addToOwnership(ag.getAddressIfSet());
      m_ourAgent.commit();
      // We now have a pointer to the agent, we can make the ownership official
      ag.setOwner(m_ourAgent.getAddressIfSet());
      ag.commit();
      // And we can remove the now dangling pointer from the agent register
      // (we hold an exclusive lock all along)
      m_agentRegister.trackAgent(ag.getAddressIfSet());
      m_agentRegister.commit();
      // Agent is officially our, we can remove it from the untracked agent's
      // list
      m_agentRegister.trackAgent(ag.getAddressIfSet());
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
 
void GarbageCollector::checkHeartbeats() {
  // Check the heartbeats of the watched agents
  // We can still fail on many steps
  for (std::map<std::string, AgentWatchdog * >::iterator wa = m_watchedAgents.begin();
      wa != m_watchedAgents.end();) {
    // Get the heartbeat. Clean dead agents and remove references to them
    if (!wa->second->checkAlive()) {
      cleanupDeadAgent(wa->first);
      ScopedExclusiveLock oaLock(m_ourAgent);
      m_ourAgent.removeFromOwnership(wa->first);
      m_ourAgent.commit();
      delete wa->second;
      m_watchedAgents.erase(wa++);
    } else {
      wa++;
    }
  }
}

 void GarbageCollector::cleanupDeadAgent(const std::string & name) {
   // Check that we are still owners of the agent (sanity check).
   Agent agent(name, m_objectStore);
   ScopedExclusiveLock agLock(agent);
   agent.fetch();
   if (agent.getOwner() != m_ourAgent.getAddressIfSet()) {
     throw cta::exception::Exception("In GarbageCollector::cleanupDeadAgent: the ownership is not ours as expected");
   }
   // Return all objects owned by the agent to their respective backup owners
   auto ownedObjects = agent.getOwnershipList();
   for (auto obj = ownedObjects.begin(); obj!= ownedObjects.end(); obj++) {
     // Find the object
     GenericObject go(*obj, m_objectStore);
     // If the object does not exist, we're done.
     if (go.exists()) {
       ScopedExclusiveLock goLock(go);
       go.fetch();
       // Call GenericOpbject's garbage collect method, which in turn will
       // delegate to the object type's garbage collector.
       go.garbageCollect(goLock);
     }
     // In all cases, relinquish ownership for this object
     agent.removeFromOwnership(*obj);
     agent.commit();
   }
   // We now processed all the owned objects. We can delete the agent's entry
   agent.removeAndUnregisterSelf();
 }



}}

#pragma once

#include "AgentVisitor.hpp"
#include "RootEntry.hpp"
#include "RecallJob.hpp"
#include "JobPool.hpp"
#include "FIFO.hpp"
#include "AgentRegister.hpp"
#include "Counter.hpp"
#include <unistd.h>

namespace cta { namespace objectstore {

class Action {
public:
  virtual void execute() {}
  virtual ~Action() {}
};

class JobPoster: public Action {
public:
  JobPoster (Agent & agent, int number, int objective):
    m_agent(agent), m_number(number), m_objective(objective), m_achieved(0){}
  virtual void execute() {
    std::stringstream name;
    name << "JobPoster-" << m_number;
    m_agent.setup(name.str());
    m_agent.create();
    RootEntry re(m_agent);
    JobPool jp(re.getJobPool(m_agent), m_agent);
    std::string fifoName = jp.getRecallFIFO(m_agent);
    FIFO fifo(fifoName, m_agent);
    std::cout << name.str() << " starting" << std::endl;
    while (m_achieved < m_objective) {
      m_agent.heartbeat(m_agent);
      std::stringstream src, dst;
      src << "S-" << m_number << "-" << m_achieved;
      dst << "D-" << m_number << "-" << m_achieved;
      // This create the recall job and return the name. It is
      // automatically added to the creation intent log of m_agent.
      std::string rjName = RecallJob::create(fifoName, src.str(), dst.str(), m_agent);
      // Post the job
      fifo.push(rjName, m_agent);
      // We can now release it from the intent log
      m_agent.removeFromIntent(fifoName, rjName, serializers::RecallJob_t);
      std::cout << rjName << " created" << std::endl;
      m_achieved++;
    }
    std::cout << name.str() << " complete" << std::endl;
  }
private:
  Agent & m_agent;
  int m_number;
  int m_objective;
  int m_achieved;
};

class Recaller: public Action {
public:
  Recaller (Agent & agent, int number): m_agent(agent), m_number(number) {}
  virtual void execute() {
    std::stringstream name;
    name << "RecallerAction-" << m_number;
    m_agent.setup(name.str());
    m_agent.create();
    RootEntry re(m_agent);
    JobPool jp(re.getJobPool(m_agent), m_agent);
    FIFO fifo(jp.getRecallFIFO(m_agent), m_agent);
    Counter rc(jp.getRecallCounter(m_agent), m_agent);
    std::cout << name.str() << " starting" << std::endl;
    cta::utils::Timer timeout;
    while (true) {
      try {
        m_agent.heartbeat(m_agent);
        // Pop a job from the FIFO
        FIFO::Transaction tr = fifo.startTransaction(m_agent);
        std::string rjName = tr.peek();
        m_agent.addToOwnership(rjName, serializers::RecallJob_t);
        tr.popAndUnlock();
        timeout.reset();
        RecallJob rj(rjName, m_agent);
        // Sleep on it for a while
        usleep(100 * 1000);
        // Log the deletion
        std::cout << "RecallJob " << rj.source(m_agent) << " => " 
                  << rj.destination(m_agent) << " is done by recaller " 
                  << m_number << " (tid=" << syscall(SYS_gettid) << ")" 
                  << std::endl;
        rj.remove();
        // Count the deletion
        rc.inc(m_agent);
        m_agent.removeFromOwnership(rjName, serializers::RecallJob_t);
      } catch (FIFO::FIFOEmpty &) {
        if (timeout.secs() > 1.0) {
          std::cout << "The recall FIFO was empty for more than a second. Exiting." << std::endl;
          break;
        }
        usleep(100 * 1000);
      } catch (std::exception&) {
      } catch (...) {
        throw;
      }
    }
  }

private:
  Agent & m_agent;
  int m_number;
};

class GarbageCollector: public Action {
public:
  GarbageCollector (Agent & agent): m_agent(agent) {}
  virtual void execute() {
    std::stringstream name;
    name << "GarbageCollector";
    m_agent.setup(name.str());
    m_agent.create();
    RootEntry re(m_agent);
    AgentRegister ar(re.getAgentRegister(m_agent), m_agent);
    std::cout << m_agent.name() << " starting" << std::endl;
    utils::Timer noAgentTimer;
    std::map<std::string, AgentWatchdog *> watchdogs;
    while (true) {
      m_agent.heartbeat(m_agent);
      // Get the list of current agents
      std::list<std::string> agentNames = ar.getElements(m_agent);
      // If no one is running, go away after a delay
      if(!agentNames.size()) {
        if (noAgentTimer.secs() > 1.0)
          break;
      } else {
        noAgentTimer.reset();
      }
      // On a first pass, trim the watchdog list of now-gone agents
      for (std::map<std::string, AgentWatchdog *>::iterator i=watchdogs.begin();
          i != watchdogs.end();) {
        if (std::find(agentNames.begin(), agentNames.end(), i->first) == agentNames.end()) {
          std::cout << "Stopping to watch completed agent: " << i->first << std::endl;
          delete i->second;
          // The post-increment returns the iterator to erase, 
          // but sets i to the next value, which will remain valid.
          watchdogs.erase(i++);
        } else {
          i++;
        }
      }
      // On a second pass, check that we are acquainted with all processes
      for (std::list<std::string>::iterator i=agentNames.begin(); 
           i != agentNames.end(); i++) {
        if(watchdogs.find(*i) == watchdogs.end()) {
          try {
            std::cout << "Trying to check for agent " << *i << "... ";
            watchdogs[*i] = new AgentWatchdog(*i, m_agent);
            std::cout << "OK" << std::endl;
          } catch ( cta::exception::Exception & ) {
            std::cout << "The agent structure is gone. Removing it from register." << std::endl;
            ar.removeElement(*i, m_agent);
          }
        }
      }
      // And now check the heartbeats of the agents
      for (std::map<std::string, AgentWatchdog *>::iterator i=watchdogs.begin();
          i != watchdogs.end();) {
        try {
          if (!i->second->checkAlive(m_agent)) {
            collectGarbage(i->first);
            delete i->second;
            // Delete the agent record and de-reference it
            std::cout << "Deleting agent " << i->first << " and removing it from register" << std::endl;
            try { m_agent.objectStore().remove(i->first); } catch (std::exception &) {}
            try { ar.removeElement(i->first, m_agent); } catch (std::exception &) {}
            // The post-increment returns the iterator to erase, 
            // but sets i to the next value, which will remain valid.
            watchdogs.erase(i++);
          } else {
            i++;
          }
        } catch ( cta::exception::Exception & ) {
          delete i->second;
          watchdogs.erase(i++);
        }
      }
      usleep(100*1000);
    }
  }
                
private:
  Agent & m_agent;
  void collectGarbage(const std::string & agentName)  {
    // When collecting the garbage of an agent, we have to iterate through its
    // intended and owned objects, validate that they still exist, are owned by 
    // the dead agent, and re-post them to the container where they should be.
    try {
      std::cout << "In garbage collector, found a dead agent: " << agentName << std::endl;
      // If the agent entry does not exist anymore, we're done.
      // print the recall FIFO
      std::cout << "Recall FIFO before garbage collection:" << std::endl;
      try {
        RootEntry re(m_agent);
        JobPool jp(re.getJobPool(m_agent), m_agent);
        FIFO rf(jp.getRecallFIFO(m_agent), m_agent);
        std::cout << rf.dump(m_agent) << std::endl;
      } catch (std::exception&) {
      } 
      AgentVisitor ag(agentName, m_agent);
      // Iterate on intended objects
      std::list<AgentVisitor::intentEntry> intendedObjects = ag.getIntentLog(m_agent);
      for (std::list<AgentVisitor::intentEntry>::iterator i=intendedObjects.begin();
            i != intendedObjects.end(); i++) {
        collectIntendedObject(*i);
      }
      // Iterate on owned objects
      std::list<AgentVisitor::ownershipEntry> ownedObjects = ag.getOwnershipLog(m_agent);
      for (std::list<AgentVisitor::ownershipEntry>::iterator i=ownedObjects.begin();
              i != ownedObjects.end(); i++) {
        collectOwnedObject(*i);
        std::cout << "Considering owned object " << i->name 
                << " (type:" << i->objectType << ")" << std::endl;
      }
      // print the recall FIFO
      std::cout << "Recall FIFO after garbage collection:" << std::endl;
      try {
        RootEntry re(m_agent);
        JobPool jp(re.getJobPool(m_agent), m_agent);
        FIFO rf(jp.getRecallFIFO(m_agent), m_agent);
        std::cout << rf.dump(m_agent) << std::endl;
      } catch (std::exception&) {
      }
    } catch (std::exception&) {
    }
  }
  
  void collectIntendedObject(AgentVisitor::intentEntry & i) {
    std::cout << "Considering intended object " << i.name << " (container:" << i.container
            << " type:" << i.objectType << ")" << std::endl;
    switch (i.objectType) {
      case serializers::JobPool_t:
      {
        // We need to check whether the job pool is plugged into the
        // root entry and just discard it if it is not.
        RootEntry re(m_agent);
        std::string jopPoolName;
        try {
          jopPoolName = re.getJobPool(m_agent);
        } catch (std::exception&) {
          // If we cannot find a reference to any job pool, then
          // this one is not referenced
          std::cout << "Could not get the job pool name from the root entry."
                  << " Deleting this job pool." << std::endl;
          m_agent.objectStore().remove(i.name);
          break;
        }
        if (jopPoolName != i.name) {
          std::cout << "This job pool is not the one referenced in the root entry. (root's="
                  << jopPoolName << ")"
                  << " Deleting this job pool." << std::endl;
          m_agent.objectStore().remove(i.name);
        }
        break;
      }
      case serializers::RecallFIFO_t:
        // We need to check that this recall FIFO is plugged in the job pool
        // structure
      {
        RootEntry re(m_agent);
        std::string recallFIFOName;
        try {
          JobPool jp(re.getJobPool(m_agent), m_agent);
          recallFIFOName = jp.getRecallFIFO(m_agent);
        } catch (std::exception&) {
          std::cout << "Could not get the recall FIFO name from the job pool."
                  << " Deleting this recallFIFO." << std::endl;
          // If we cannot find a reference to any recall FIFO, then
          // this one is not referenced
          m_agent.objectStore().remove(i.name);
          break;
        }
        if (recallFIFOName != i.name) {
          std::cout << "This recallFIFO is not the one referenced in the job pool entry. (jp's="
                  << recallFIFOName << ")"
                  << " Deleting this recall FIFO." << std::endl;
          m_agent.objectStore().remove(i.name);
        }
        break;
      }
      case serializers::RecallJob_t:
      {
        // The recall job here can only think of on FIFO it could be in. This
        // will be more varied in the future.
        RecallJob rj(i.name, m_agent);
        std::string owner = rj.owner(m_agent);
        std::cout << "Will post the recall job back to its owner FIFO." << std::endl;
        FIFO RecallFIFO(owner, m_agent);
        // We repost the job unconditionally in the FIFO. In case of an attempted
        // pop, the actual ownership (should be the FIFO) will be checked by
        // the consumer. If the owner is not right, FIFO entry will simply be 
        // discarded.
        RecallFIFO.push(i.name, m_agent);
        break;
      }
      case serializers::RootEntry_t:
        std::cout << "Unexpected entry type in garbage collection: RootEntry" << std::endl;
        break;
      case serializers::AgentRegister_t:
        std::cout << "Unexpected entry type in garbage collection: AgentRegister" << std::endl;
        break;
      case serializers::Agent_t:
        std::cout << "Unexpected entry type in garbage collection: Agent" << std::endl;
        break;
      case serializers::MigrationFIFO_t:
        std::cout << "Unexpected entry type in garbage collection: MigrationFIFO" << std::endl;
        break;
      case serializers::Counter_t:
        std::cout << "Unexpected entry type in garbage collection: Counter" << std::endl;
        break;
    }
  }
  
  void collectOwnedObject(AgentVisitor::ownershipEntry & i) {
    std::cout << "Considering owned object " << i.name 
            << " (type:" << i.objectType << ")" << std::endl;
    switch (i.objectType) {
      case serializers::JobPool_t:
        std::cout << "Unexpected entry type in garbage collection: JobPool" << std::endl;
        break;
      case serializers::RecallFIFO_t:
        std::cout << "Unexpected entry type in garbage collection: RecallFIFO" << std::endl;
        break;
      case serializers::RecallJob_t:
      {
        // The recall job here can only think of on FIFO it could be in. This
        // will be more varied in the future.
        RecallJob rj(i.name, m_agent);
        std::string owner = rj.owner(m_agent);
        std::cout << "Will post the recall job back to its owner FIFO: "  << owner << std::endl;
        FIFO RecallFIFO(owner, m_agent);
        // We repost the job unconditionally in the FIFO. In case of an attempted
        // pop, the actual ownership (should be the FIFO) will be checked by
        // the consumer. If the owner is not right, FIFO entry will simply be 
        // discarded.
        RecallFIFO.push(i.name, m_agent);
        break;
      }
      case serializers::RootEntry_t:
        std::cout << "Unexpected entry type in garbage collection: RootEntry" << std::endl;
        break;
      case serializers::AgentRegister_t:
        std::cout << "Unexpected entry type in garbage collection: AgentRegister" << std::endl;
        break;
      case serializers::Agent_t:
        std::cout << "Unexpected entry type in garbage collection: Agent" << std::endl;
        break;
      case serializers::MigrationFIFO_t:
        std::cout << "Unexpected entry type in garbage collection: MigrationFIFO" << std::endl;
        break;
      case serializers::Counter_t:
        std::cout << "Unexpected entry type in garbage collection: Counter" << std::endl;
        break;
    }
  }
};

}}



#pragma once

#include "AgentVisitor.hpp"
#include "RootEntry.hpp"
#include "RecallJob.hpp"
#include "JobPool.hpp"
#include "FIFO.hpp"
#include "AgentRegister.hpp"
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
    std::cout << name.str() << " starting" << std::endl; 
    while (true) {
      try {
        m_agent.heartbeat(m_agent);
        // Pop a job from the FIFO
        FIFO::Transaction tr = fifo.startTransaction(m_agent);
        std::string rjName = tr.peek();
        m_agent.addToOwnership(tr.peek(), serializers::RecallJob_t);
        tr.popAndUnlock();
        RecallJob rj(rjName, m_agent);
        // Sleep on it for a while
        usleep(100 * 1000);
        // Log the deletion
        std::cout << "RecallJob " << rj.source(m_agent) << " => " 
                  << rj.destination(m_agent) << " is done" << std::endl;
        rj.remove();
      } catch (FIFO::FIFOEmpty &) {
        cta::utils::Timer timeout;
        while (timeout.secs() < 1.0) {
          try {
            if (fifo.size(m_agent) > 0) {
              continue;
            } else {
              usleep (100 * 1000);
            }
          } catch (abi::__forced_unwind&) {
            throw;
          } catch (...) {}
        }
        std::cout << name.str() << " complete: FIFO empty" << std::endl;
        break; 
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
    std::cout << name.str() << " starting" << std::endl;
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
          delete i->second;
          // The post-increment returns the iterator to erase, 
          // but sets i to the next value, which will remain valid.
          watchdogs.erase(i++);
        } else {
          i++;
        }
      }
      // On a second pass, check that we are acquaint with all processes
      for (std::list<std::string>::iterator i=agentNames.begin(); 
           i != agentNames.end(); i++) {
        if(watchdogs.find(*i) == watchdogs.end()) {
          watchdogs[*i] = new AgentWatchdog(*i, m_agent);
        }
      }
      // And now check the heartbeats of the agents
      for (std::map<std::string, AgentWatchdog *>::iterator i=watchdogs.begin();
          i != watchdogs.end();) {
        if (!i->second->checkAlive(m_agent)) {
          collectGarbage(i->first);
          delete i->second;
          // The post-increment returns the iterator to erase, 
          // but sets i to the next value, which will remain valid.
          watchdogs.erase(i++);
        }
      }
    }
  }
                
private:
  Agent & m_agent;
  void collectGarbage(const std::string & agentName) {
    // When collecting the garbage of an agent, we have to iterate through its
    // intended and owned objects, validate that they still exist, are owned by 
    // the dead agent, and re-post them to the container where they should be.
    try {
      // If the agent entry does not exist anymore, we're done.
      AgentVisitor ag(agentName, m_agent);
      std::list<AgentVisitor::intentEntry> intendedObjects = ag.getIntentLog(m_agent);
      for (std::list<AgentVisitor::intentEntry>::iterator i=intendedObjects.begin();
            i != intendedObjects.end(); i++) {
        switch (i->objectType) {
          case serializers::JobPool_t:
          {
            // We need to check whether the job pool is plugged into the
            // root entry and just discard it if it is not.
            RootEntry re(m_agent);
            std::string jopPoolName;
            try {
              jopPoolName = re.getJobPool(m_agent);
            } catch (abi::__forced_unwind&) {
              throw;
            } catch (...) {
              // If we cannot find a reference to any job pool, then
              // this one is not referenced
              m_agent.objectStore().remove(i->name);
              break;
            }
            if (jopPoolName != i->name)
              m_agent.objectStore().remove(i->name);
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
            } catch (abi::__forced_unwind&) {
              throw;
            } catch (...) {
              // If we cannot find a reference to any recall FIFO, then
              // this one is not referenced
              m_agent.objectStore().remove(i->name);
              break;
            }
            if (recallFIFOName != i->name)
              m_agent.objectStore().remove(i->name);
            break;
          }
          case serializers::RecallJob_t:
          {
            // The recall job here can only think of on FIFO it could be in. This
            // will be more varied in the future.
            RecallJob rj(i->name, m_agent);
            FIFO RecallFIFO(rj.owner(m_agent), m_agent);
            // We repost the job unconditionally in the FIFO. In case of an attempted
            // pop, the actual ownership (should be the FIFO) will be checked by
            // the consumer. If the owner is not right, FIFO entry will simply be 
            // discarded.
            RecallFIFO.push(i->name, m_agent);
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
        }
      }
    } catch (abi::__forced_unwind&) {
            throw;
    } catch (...) {}
  }
};

}}



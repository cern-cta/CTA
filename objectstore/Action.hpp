#pragma once

#include "Agent.hpp"
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
    std::cout << name << " starting";
    while (m_achieved < m_objective) {
      std::stringstream src, dst;
      src << "S-" << m_number << "-" << m_achieved;
      dst << "D-" << m_number << "-" << m_achieved;
      // This create the recall job and return the name. It is
      // automatically added to the creation intent log of m_agent.
      std::string rjName = RecallJob::create(fifoName, src.str(), dst.str(), m_agent);
      // Post the job
      fifo.push(rjName, m_agent);
      // We can now release it from the intent log
      m_agent.removeFromIntent(fifoName, rjName, "RecallJob");
      std::cout << rjName << " created" << std::endl;
      m_achieved++;
    }
    std::cout << name << " complete" << std::endl;
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
    std::cout << name << " starting";
    while (true) {
      try {
        // Pop a job from the FIFO
        FIFO::Transaction tr = fifo.startTransaction(m_agent);
        std::string rjName = tr.peek();
        m_agent.addToOwnership(tr.peek(), "RecallJob");
        tr.popAndUnlock();
        RecallJob rj(rjName, m_agent);
        // Sleep on it for a while
        usleep(100 * 1000);
        // Log the deletion
        std::cout << "RecallJob " << rj.source(m_agent) << " => " 
                  << rj.destination(m_agent) << " is done" << std::endl;
        rj.remove();
      } catch (FIFO::FIFOEmpty &) { break; }
      std::cout << name << "complete: FIFO empty" << std::endl;
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
    std::cout << name << " starting";
    utils::Timer noAgentTimer;
    std::map<std::string, AgentWatchdog *> watchdogs;
    while (true) {
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
    // intended and owned objects, validate that they are still owned by the dead
    // agent, and re-post them to the container where they should be (and ownership)
    // is re-set to the container.
    Agent ag(agentName, m_agent);
    std::list<Agent::intentEntry> intendedObjects = ag.getIntentLog();
  }
};

}}



#pragma once

#include "RootEntry.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include "JobPool.hpp"
#include "Counter.hpp"
#include "AgentVisitor.hpp"
#include <iostream>

namespace cta { namespace objectstore {

class ObjectStructureDumper {
public:
  std::string dump(Agent & agent) {
    std::stringstream ret;
    ret << "<< Structure dump start" << std::endl;
    RootEntry re(agent);
    ret << re.dump(agent) << std::endl;;
    try {
      AgentRegister ar(re.getAgentRegister(agent), agent);
      ret << ar.dump(agent) << std::endl;
      std::list<std::string> agList = ar.getElements(agent);
      for (std::list<std::string>::iterator i=agList.begin(); i!=agList.end(); i++) {
        AgentVisitor a(*i, agent);
        ret << a.dump(agent) << std::endl;
      }
    } catch (RootEntry::NotAllocatedEx &) {}
    try {
      JobPool jp (re.getJobPool(agent), agent);
      ret << jp.dump(agent) << std::endl;
      try {
        FIFO rf(jp.getRecallFIFO(agent), agent);
        ret << rf.dump(agent) << std::endl;
      } catch (std::exception&) {
      }
      try {
        Counter rc(jp.getRecallCounter(agent), agent);
        ret << "Recall Counter=" << rc.get(agent) << std::endl;
      } catch (std::exception&) {
      }
    } catch (RootEntry::NotAllocatedEx &) {}
    ret << ">> Structure dump end" << std::endl;
    return ret.str();
  }
};

}}
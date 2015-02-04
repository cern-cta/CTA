#pragma once

#include "RootEntry.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include "JobPool.hpp"
#include <iostream>

namespace cta { namespace objectstore {

class ObjectStrucutreDumper {
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
      } catch (abi::__forced_unwind&) {
            throw;
      } catch (...) {}
    } catch (RootEntry::NotAllocatedEx &) {}
    ret << ">> Structure dump end" << std::endl;
    return ret.str();
  }
};

}}
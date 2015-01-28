#pragma once

#include "RootEntry.hpp"
#include "AgentRegister.hpp"
#include "Agent.hpp"
#include <iostream>

class ObjectStrucutreDumper {
public:
  std::string dump(Agent & agent) {
    std::stringstream ret;
    ret << "<< Structure dump start" << std::endl;
    RootEntry re(agent);
    ret << re.dump(agent);
    try {
      AgentRegister ar(re.getAgentRegister(agent), agent);
      ret << ar.dump("root->agentRegister", agent);
      std::list<std::string> agList = ar.getElements(agent);
      for (std::list<std::string>::iterator i=agList.begin(); i!=agList.end(); i++) {
        Agent a(*i, agent);
        ret << a.dump(agent);
      }
    } catch (RootEntry::NotAllocatedEx &) {}
    ret << ">> Structure dump end" << std::endl;
    return ret.str();
  }
};
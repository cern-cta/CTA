#pragma once

#include "RootEntry.hpp"
#include "Register.hpp"
#include <iostream>

class ObjectStrucutreDumper {
public:
  std::string dump(Agent & agent) {
    std::stringstream ret;
    ret << "<< Structure dump start" << std::endl;
    RootEntry re(agent);
    ret << re.dump(agent);
    try {
      Register ar(re.getAgentRegister(agent), agent);
      ret << ar.dump("root->agentRegister", agent);
    } catch (RootEntry::NotAllocatedEx &) {}
    ret << ">> Structure dump end" << std::endl;
    return ret.str();
  }
};
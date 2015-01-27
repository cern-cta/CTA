#pragma once

#include "RootEntry.hpp"
#include "Register.hpp"
#include <iostream>

class ObjectStrucutreDumper {
public:
  std::string dump(ObjectStore & os, ContextHandle & context) {
    std::stringstream ret;
    ret << "<< Structure dump start" << std::endl;
    RootEntry re(os, context);
    ret << re.dump(context);
    try {
      Register ar(os, re.getAgentRegister(context), context);
      ret << ar.dump("root->agentRegister", context);
    } catch (RootEntry::NotAllocatedEx &) {}
    ret << ">> Structure dump end" << std::endl;
    return ret.str();
  }
};
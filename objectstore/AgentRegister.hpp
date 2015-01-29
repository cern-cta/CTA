#pragma once

#include "ObjectOps.hpp"
#include "Agent.hpp"
#include <algorithm>

namespace cta { namespace objectstore {

class AgentRegister: private ObjectOps<serializers::AgentRegister> {
public:
  AgentRegister(const std::string & name, Agent & agent);
  void addElement (std::string name, Agent & agent);
  void removeElement (const std::string  & name, Agent & agent);
  void addIntendedElement (std::string name, Agent & agent);
  void upgradeIntendedElementToActual(std::string name, Agent & agent);
  void removeIntendedElement (const std::string  & name, Agent & agent);
  std::list<std::string> getElements(Agent & agent);
  std::string dump(Agent & agent);
};

}}
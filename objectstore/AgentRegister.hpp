#pragma once

#include "ObjectOps.hpp"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;

class AgentRegister: public ObjectOps<serializers::AgentRegister> {
public:
  AgentRegister(const std::string & name, Backend & os);
  void addElement (std::string name);
  void removeElement (const std::string  & name);
  void addIntendedElement (std::string name);
  void upgradeIntendedElementToActual(std::string name);
  void removeIntendedElement (const std::string  & name);
  std::list<std::string> getElements();
  std::string dump();
};

}}
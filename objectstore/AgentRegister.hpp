#pragma once

#include "ObjectOps.hpp"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;

class AgentRegister: public ObjectOps<serializers::AgentRegister> {
public:
  AgentRegister(Backend & os);
  AgentRegister(const std::string & name, Backend & os);
  void initialize();
  void addAgent (std::string name);
  void removeAgent (const std::string  & name);
  void trackAgent (std::string name);
  void untrackAgent(std::string name);
  std::list<std::string> getAgents();
  std::list<std::string> getUntrackedAgents();
  std::string dump();
};

}}